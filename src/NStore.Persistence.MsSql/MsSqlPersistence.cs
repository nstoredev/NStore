using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.BaseSqlPersistence;
using NStore.Core.Logging;
using NStore.Core.Persistence;

namespace NStore.Persistence.MsSql
{
    public class MsSqlPersistence : AbstractSqlPersistence, IPersistence
    {
        private const int DuplicatedIndexExceptionErrorNumber = 2601;

        private readonly MsSqlPersistenceOptions _options;
        private readonly INStoreLogger _logger;

        public bool SupportsFillers => false;

        public MsSqlPersistence(MsSqlPersistenceOptions options) : base(options)
        {
            _options = options;
            _logger = options.LoggerFactory.CreateLogger($"MsSqlPersistence-{options.StreamsTableName}");

            if (_options.Serializer == null)
            {
                throw new MsSqlPersistenceException("MsSqlOptions should provide a custom Serializer");
            }
        }

        /// <summary>
        /// Executes an operation with retry logic for transient SQL errors.
        /// </summary>
        private async Task<T> ExecuteWithRetryAsync<T>(
            Func<Task<T>> operation,
            CancellationToken cancellationToken,
            string operationName = null)
        {
            int attempt = 0;
            while (true)
            {
                try
                {
                    return await operation().ConfigureAwait(false);
                }
                catch (SqlException ex) when (IsTransientError(ex) && attempt < _options.MaxRetryAttempts)
                {
                    attempt++;
                    var delay = _options.RetryDelayMilliseconds * (int)Math.Pow(2, attempt - 1);
                    
                    _logger.LogWarning(
                        $"Transient SQL error in {operationName ?? "operation"} (attempt {attempt}/{_options.MaxRetryAttempts}): {ex.Message}. Retrying in {delay}ms...");
                    
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
                catch (SqlException ex)
                {
                    _logger.LogError($"SQL error in {operationName ?? "operation"} after {attempt} attempts: {ex.Message}\n{ex.StackTrace}");
                    throw;
                }
            }
        }

        /// <summary>
        /// Determines if a SQL exception is transient and should be retried.
        /// </summary>
        private bool IsTransientError(SqlException ex)
        {
            // Common transient error numbers for SQL Server
            // -2: Timeout
            // -1: Connection broken
            // 1205: Deadlock victim
            // 40197, 40501, 40613: Azure SQL transient errors
            // 49918, 49919, 49920: Azure SQL resource limits
            var transientErrorNumbers = new[] { -2, -1, 1205, 40197, 40501, 40613, 49918, 49919, 49920 };
            return transientErrorNumbers.Contains(ex.Number);
        }

        public async Task ReadAllAsync(
            long fromPositionInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var sql = _options.GetReadAllChunksSql(limit);

                using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
                {
                    using (var command = context.CreateCommand(sql))
                    {
                        command.CommandTimeout = _options.CommandTimeoutSeconds;
                        context.AddParam(command, "@fromPositionInclusive", fromPositionInclusive);

                        await PushToSubscriber(command, fromPositionInclusive, subscription, true, cancellationToken)
                            .ConfigureAwait(false);
                    }
                }
                return true;
            }, cancellationToken, "ReadAllAsync").ConfigureAwait(false);
        }

        public async Task InitAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug($"Initializing MsSql persistence for table '{_options.StreamsTableName}'");
            
            await ExecuteWithRetryAsync(async () =>
            {
                await EnsureTable(cancellationToken).ConfigureAwait(false);
                return true;
            }, cancellationToken, "InitAsync").ConfigureAwait(false);
            
            _logger.LogInformation($"MsSql persistence initialized for table '{_options.StreamsTableName}'");
        }

        public async Task DestroyAllAsync(CancellationToken cancellationToken)
        {
            _logger.LogWarning($"Destroying all data in table '{_options.StreamsTableName}'");
            
            await ExecuteWithRetryAsync(async () =>
            {
                using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
                {
                    var sql = _options.GetDropTableSql();
                    using (var cmd = context.CreateCommand(sql))
                    {
                        cmd.CommandTimeout = _options.CommandTimeoutSeconds;
                        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
                return true;
            }, cancellationToken, "DestroyAllAsync").ConfigureAwait(false);
            
            _logger.LogInformation($"Table '{_options.StreamsTableName}' destroyed successfully");
        }

        protected override bool IsDuplicatedStreamOperation(Exception exception)
        {
            if (exception is SqlException ex &&
                ex.Number == DuplicatedIndexExceptionErrorNumber && 
                ex.Message.Contains("_OPID"))
            {
                _logger.LogDebug($"Detected duplicate operation: {ex.Message}");
                return true;
            }
            return false;
        }

        protected override bool IsDuplicatedStreamIndex(Exception exception)
        {
            if (exception is SqlException ex &&
                ex.Number == DuplicatedIndexExceptionErrorNumber && 
                ex.Message.Contains("_IDX"))
            {
                _logger.LogDebug($"Detected duplicate stream index: {ex.Message}");
                return true;
            }
            return false;
        }

        public async Task ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            if (partitionRequests is null)
            {
                throw new ArgumentNullException(nameof(partitionRequests));
            }

            var requests = partitionRequests.ToList();
            if (!requests.Any())
            {
                return;
            }

            // Validate partition requests
            foreach (var request in requests)
            {
                if (string.IsNullOrWhiteSpace(request.PartitionId))
                {
                    throw new ArgumentException("PartitionId cannot be null or whitespace", nameof(partitionRequests));
                }

                if (request.ToPartitionIndexInclusive < request.FromPartitionIndexInclusive)
                {
                    throw new ArgumentException(
                        $"ToPartitionIndexInclusive ({request.ToPartitionIndexInclusive}) must be >= FromPartitionIndexInclusive ({request.FromPartitionIndexInclusive}) for partition {request.PartitionId}",
                        nameof(partitionRequests));
                }
            }

            var sql = _options.GetRangeMultiplePartitionWithRangesSelectChunksSql(requests);

            using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
            {
                using (var command = context.CreateCommand(sql))
                {
                    // Add parameters for each partition request
                    for (int i = 0; i < requests.Count; i++)
                    {
                        var request = requests[i];
                        context.AddParam(command, $"@p{i}", request.PartitionId);

                        if (request.FromPartitionIndexInclusive > 0)
                        {
                            context.AddParam(command, $"@from{i}", request.FromPartitionIndexInclusive);
                        }

                        if (request.ToPartitionIndexInclusive != long.MaxValue)
                        {
                            context.AddParam(command, $"@to{i}", request.ToPartitionIndexInclusive);
                        }
                    }

                    // Use the minimum from index as the starting position for subscription
                    var startIndex = requests.Min(r => r.FromPartitionIndexInclusive);
                    await PushToSubscriber(command, startIndex, subscription, false, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }

#if NET8_0_OR_GREATER
        public async IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (partitionRequests is null)
            {
                throw new ArgumentNullException(nameof(partitionRequests));
            }

            var requests = partitionRequests.ToList();
            if (!requests.Any())
            {
                yield break;
            }

            var recorder = new Recorder();
            await ReadForwardMultiplePartitionsWithRangesAsync(requests, recorder, cancellationToken)
                .ConfigureAwait(false);

            foreach (var chunk in recorder.Chunks)
            {
                yield return chunk;
            }
        }
#else
        public IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("IAsyncEnumerable is only supported in .NET 8.0 or greater");
        }
#endif
    }
}