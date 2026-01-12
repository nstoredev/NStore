using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Microsoft.Data.Sqlite;
using System.Threading;
using System.Threading.Tasks;
using NStore.BaseSqlPersistence;
using NStore.Core.Logging;
using NStore.Core.Persistence;

namespace NStore.Persistence.Sqlite
{
    public class SqlitePersistence : AbstractSqlPersistence, IPersistence, IEnhancedPersistence, IAsyncDisposable, IDisposable
    {
        private bool _disposed = false;
        private const int DUPLICATED_INDEX_EXCEPTION = 19;
        private const int SQLITE_BUSY = 5;
        private const int SQLITE_LOCKED = 6;
        private const int SQLITE_IOERR = 10;
        private const int SQLITE_CORRUPT = 11;
        private const int SQLITE_FULL = 13;
        private const int SQLITE_CANTOPEN = 14;
        private const int SQLITE_PROTOCOL = 15;
        private const int SQLITE_READONLY = 8;

        private const int MAX_RETRY_ATTEMPTS = 3;
        private const int RETRY_DELAY_MILLISECONDS = 100;
        private const int MAX_PARTITIONS_WARNING_THRESHOLD = 20;

        private readonly SqlitePersistenceOptions _options;
        private readonly INStoreLogger _logger;

        public bool SupportsFillers => false;

        ~SqlitePersistence()
        {
            Dispose(false);
        }

        public SqlitePersistence(SqlitePersistenceOptions options) : base(options)
        {
            _options = options;
            _logger = _options.LoggerFactory.CreateLogger($"SqlitePersistence-{options.StreamsTableName}");

            if (_options.Serializer == null)
            {
                throw new SqlitePersistenceException("SqliteOptions should provide a custom Serializer");
            }
        }

        private async Task<IList<IChunk>> LoadBuffer(DbCommand command, CancellationToken cancellationToken)
        {
            var list = new List<IChunk>();
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    list.Add(ReadChunk(reader));
                }
            }

            return list;
        }

        public async Task ReadAllAsync(
            long fromPositionInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            var top = Math.Min(limit, 200);
            var sql = _options.GetReadAllChunksSql(top);

            await subscription.OnStartAsync(fromPositionInclusive).ConfigureAwait(false);
            var lastPosition = fromPositionInclusive;
            int readcount = 0;

            try
            {
                while (true)
                {
                    IList<IChunk> buffer = null;

                    using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
                    {
                        using (var command = context.CreateCommand(sql))
                        {
                            context.AddParam(command, "@fromPositionInclusive", fromPositionInclusive);
                            buffer = await LoadBuffer(command, cancellationToken).ConfigureAwait(false);
                        }
                    }

                    if (buffer.Count == 0)
                    {
                        await subscription.CompletedAsync(lastPosition).ConfigureAwait(false);
                        return;
                    }

                    foreach (var chunk in buffer)
                    {
                        readcount++;

                        lastPosition = chunk.Position;
                        if (!await subscription.OnNextAsync(chunk).ConfigureAwait(false))
                        {
                            await subscription.StoppedAsync(chunk.Position).ConfigureAwait(false);
                            return;
                        }

                        if (readcount == limit)
                        {
                            await subscription.CompletedAsync(lastPosition).ConfigureAwait(false);
                            return;
                        }
                    }

                    fromPositionInclusive = lastPosition + 1;
                }
            }
            catch (Exception e)
            {
                await subscription.OnErrorAsync(lastPosition, e).ConfigureAwait(false);
            }
        }

        protected override bool IsDuplicatedStreamOperation(Exception exception)
        {
            return exception is SqliteException ex &&
                   ex.SqliteErrorCode == DUPLICATED_INDEX_EXCEPTION &&
                   ex.Message.Contains(".PartitionId") &&
                   ex.Message.Contains(".OperationId");
        }

        protected override bool IsDuplicatedStreamIndex(Exception exception)
        {
            return exception is SqliteException ex &&
                   ex.SqliteErrorCode == DUPLICATED_INDEX_EXCEPTION &&
                   ex.Message.Contains(".PartitionId") &&
                   ex.Message.Contains(".Index");
        }

        /// <summary>
        /// Determines if a SQLite exception is transient and should be retried.
        /// </summary>
        /// <param name="ex">The SQLite exception to check</param>
        /// <returns>True if the error is transient and can be retried</returns>
        private bool IsTransientError(SqliteException ex)
        {
            // SQLITE_BUSY: Database is locked by another connection
            // SQLITE_LOCKED: A table in the database is locked
            // SQLITE_IOERR: Disk I/O error occurred
            // SQLITE_PROTOCOL: Protocol error (usually transient)
            var transientErrorCodes = new[] { SQLITE_BUSY, SQLITE_LOCKED, SQLITE_IOERR, SQLITE_PROTOCOL };
            return transientErrorCodes.Contains(ex.SqliteErrorCode);
        }

        /// <summary>
        /// Executes an operation with retry logic for transient SQLite errors.
        /// </summary>
        /// <typeparam name="T">Return type of the operation</typeparam>
        /// <param name="operation">The operation to execute</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="operationName">Name of the operation for logging purposes</param>
        /// <returns>Result of the operation</returns>
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
                catch (SqliteException ex) when (IsTransientError(ex) && attempt < MAX_RETRY_ATTEMPTS)
                {
                    attempt++;
                    var delay = RETRY_DELAY_MILLISECONDS * (int)Math.Pow(2, attempt - 1);
                    
                    _logger.LogWarning(
                        $"Transient SQLite error in {operationName ?? "operation"} (attempt {attempt}/{MAX_RETRY_ATTEMPTS}): {ex.Message}. Retrying in {delay}ms...");
                    
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
                catch (SqliteException ex)
                {
                    _logger.LogError($"SQLite error in {operationName ?? "operation"} after {attempt} attempts: {ex.Message}\n{ex.StackTrace}");
                    throw;
                }
            }
        }

        /// <summary>
        /// Validates partition read requests for common issues.
        /// </summary>
        /// <param name="partitionRequests">Requests to validate</param>
        /// <param name="parameterName">Parameter name for exception messages</param>
        /// <exception cref="ArgumentNullException">When partitionRequests is null</exception>
        /// <exception cref="ArgumentException">When validation fails</exception>
        private void ValidatePartitionRequests(IEnumerable<PartitionReadRequest> partitionRequests, string parameterName)
        {
            if (partitionRequests is null)
            {
                throw new ArgumentNullException(parameterName);
            }

            var requests = partitionRequests.ToList();
            
            // Check for empty partition IDs
            foreach (var request in requests)
            {
                if (string.IsNullOrWhiteSpace(request.PartitionId))
                {
                    throw new ArgumentException("PartitionId cannot be null or whitespace", parameterName);
                }

                if (request.ToPartitionIndexInclusive < request.FromPartitionIndexInclusive)
                {
                    throw new ArgumentException(
                        $"ToPartitionIndexInclusive ({request.ToPartitionIndexInclusive}) must be >= FromPartitionIndexInclusive ({request.FromPartitionIndexInclusive}) for partition {request.PartitionId}",
                        parameterName);
                }
            }

            // Check for duplicate partition IDs
            var duplicates = requests.GroupBy(r => r.PartitionId)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .ToList();

            if (duplicates.Any())
            {
                _logger.LogWarning($"Duplicate partition IDs detected in request: {string.Join(", ", duplicates)}. This may indicate a logic error.");
            }

            // Warn about performance with many partitions
            if (requests.Count > MAX_PARTITIONS_WARNING_THRESHOLD)
            {
                _logger.LogWarning($"Reading {requests.Count} partitions in a single query may impact performance. Consider batching or using a different query strategy.");
            }
        }

        public async Task InitAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            await EnsureTable(cancellationToken).ConfigureAwait(false);
        }

        public async Task DestroyAllAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
#if NET8_0_OR_GREATER
            await using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
#else
            using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
#endif
            {
                var sql = $"DROP TABLE IF EXISTS {_options.StreamsTableName} ";
                using (var cmd = context.CreateCommand(sql))
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public async Task AppendBatchAsync(WriteJob[] queue, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            var chunks = queue.Select(x =>
            {
                var chunk = new SqlChunk
                {
                    PartitionId = x.PartitionId,
                    Index = x.Index,
                    OperationId = x.OperationId ?? Guid.NewGuid().ToString(),
                    Payload = _options.Serializer.Serialize(x.Payload, out string serializerInfo),
                    SerializerInfo = serializerInfo
                };

                return chunk;
            }).ToArray();

            try
            {
                var sql = _options.GetInsertChunkSql();
#if NET8_0_OR_GREATER
                await using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
#else
                using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
#endif
                {
                    using (var transaction = context.Connection.BeginTransaction())
                    {
                        for (var index = 0; index < chunks.Length; index++)
                        {
                            var chunk = chunks[index];
                            using (var command = context.CreateCommand(sql, transaction))
                            {
                                context.AddParam(command, "@PartitionId", chunk.PartitionId);
                                context.AddParam(command, "@Index", chunk.Index);
                                context.AddParam(command, "@OperationId", chunk.OperationId);
                                context.AddParam(command, "@Payload", chunk.Payload);
                                context.AddParam(command, "@SerializerInfo", chunk.SerializerInfo);

                                try
                                {
                                    chunk.Position = (long)await command.ExecuteScalarAsync(cancellationToken)
                                        .ConfigureAwait(false);
                                }
                                catch (SqliteException ex)
                                {
                                    if (ex.SqliteErrorCode == DUPLICATED_INDEX_EXCEPTION)
                                    {
                                        if (ex.Message.Contains(".PartitionId") && ex.Message.Contains(".Index"))
                                        {
                                            queue[index].Failed(WriteJob.WriteResult.DuplicatedIndex);
                                            continue;
                                        }

                                        if (ex.Message.Contains(".PartitionId") && ex.Message.Contains(".OperationId"))
                                        {
                                            queue[index].Failed(WriteJob.WriteResult.DuplicatedOperation);
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                        transaction.Commit();
                    }
                }
            }
            catch (SqliteException ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }

            for (var index = 0; index < queue.Length; index++)
            {
                var writeJob = queue[index];
                if (writeJob.Result == WriteJob.WriteResult.None)
                {
                    writeJob.Succeeded(chunks[index]);
                }
            }
        }

        public async Task ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            ValidatePartitionRequests(partitionRequests, nameof(partitionRequests));

            var requests = partitionRequests.ToList();
            if (!requests.Any())
            {
                _logger.LogDebug("ReadForwardMultiplePartitionsWithRangesAsync called with empty partition requests");
                return;
            }

            _logger.LogDebug($"Reading {requests.Count} partition(s) with individual ranges");

            await ExecuteWithRetryAsync(async () =>
            {
                var sql = _options.GetRangeMultiplePartitionWithRangesSelectChunksSql(requests);

                if (string.IsNullOrEmpty(sql))
                {
                    _logger.LogWarning("Generated SQL query is empty");
                    return true;
                }

                _logger.LogDebug($"Generated SQL query with {requests.Count} UNION ALL clauses");

#if NET8_0_OR_GREATER
                await using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
#else
                using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
#endif
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
                return true;
            }, cancellationToken, "ReadForwardMultiplePartitionsWithRangesAsync").ConfigureAwait(false);
        }

#if NET8_0_OR_GREATER
        public async IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            ValidatePartitionRequests(partitionRequests, nameof(partitionRequests));

            var requests = partitionRequests.ToList();
            if (!requests.Any())
            {
                _logger.LogDebug("ReadForwardMultiplePartitionsWithRangesAsync (IAsyncEnumerable) called with empty partition requests");
                yield break;
            }

            _logger.LogDebug($"Reading {requests.Count} partition(s) with individual ranges (IAsyncEnumerable)");

            var sql = _options.GetRangeMultiplePartitionWithRangesSelectChunksSql(requests);

            if (string.IsNullOrEmpty(sql))
            {
                _logger.LogWarning("Generated SQL query is empty");
                yield break;
            }

            _logger.LogDebug($"Generated SQL query with {requests.Count} UNION ALL clauses");

#if NET8_0_OR_GREATER
            await using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
#else
            using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
#endif
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

                    using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            yield return ReadChunk(reader);
                        }
                    }
                }
            }
        }
#else
        public IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("IAsyncEnumerable is only supported in .NET 8.0 or greater. Please use the subscription-based overload instead.");
        }
#endif

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources asynchronously.
        /// </summary>
        /// <returns>A task that represents the asynchronous dispose operation.</returns>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _logger.LogDebug("Disposing SqlitePersistence asynchronously");
            
            await DisposeAsyncCore().ConfigureAwait(false);
            
            Dispose(false);
            GC.SuppressFinalize(this);
            
            _disposed = true;
            _logger.LogDebug("SqlitePersistence disposed successfully");
        }

        /// <summary>
        /// Performs async cleanup of managed resources.
        /// Override this in derived classes to provide custom async cleanup logic.
        /// </summary>
        protected virtual async ValueTask DisposeAsyncCore()
        {
            // SqlitePersistence doesn't hold long-lived connections,
            // but this provides a hook for derived classes
            await Task.CompletedTask.ConfigureAwait(false);
        }

        /// <summary>
        /// Performs synchronous disposal of resources.
        /// </summary>
        /// <param name="disposing">True if called from Dispose(), false if called from finalizer</param>
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                _logger.LogDebug("Disposing SqlitePersistence synchronously");
                // Dispose managed resources if any
            }

            // Dispose unmanaged resources if any
            _disposed = true;
        }

        /// <summary>
        /// Synchronous dispose method for IDisposable pattern.
        /// Prefer using DisposeAsync when possible for better async context handling.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Throws ObjectDisposedException if the object has been disposed.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when attempting to use a disposed instance</exception>
        protected void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(SqlitePersistence), "Cannot perform operations on a disposed SqlitePersistence instance.");
            }
        }
    }
}