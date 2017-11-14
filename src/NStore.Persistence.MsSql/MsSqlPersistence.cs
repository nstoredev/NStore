using System;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Logging;
using NStore.Core.Persistence;

namespace NStore.Persistence.MsSql
{
    public class MsSqlPersistence : IPersistence
    {
        private const int DuplicatedIndexExceptionErrorNumber = 2601;
        private int REFACTOR_TO_USE_SEQUENCE_OR_NOT_STRICTLY_SEQUENTIAL_VALUE = 0;

        private readonly MsSqlPersistenceOptions _options;
        private readonly INStoreLogger _logger;

        public bool SupportsFillers => false;

        public MsSqlPersistence(MsSqlPersistenceOptions options)
        {
            _options = options;
            _logger = _options.LoggerFactory.CreateLogger($"MsSqlPersistence-{options.StreamsTableName}");

            if (_options.Serializer == null)
            {
                throw new MsSqlPersistenceException("MsSqlOptions should provide a custom Serializer");
            }
        }

        private async Task ScanRange(
            string partitionId,
            long lowerIndexInclusive,
            long upperIndexInclusive,
            int limit,
            bool descending,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            var sql = _options.GetRangeSelectChunksSql(
                upperIndexInclusive: upperIndexInclusive,
                lowerIndexInclusive: lowerIndexInclusive,
                limit: limit,
                descending: descending
            );

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@PartitionId", partitionId);

                    if (lowerIndexInclusive > 0 && lowerIndexInclusive != Int64.MaxValue)
                        command.Parameters.AddWithValue("@lowerIndexInclusive", lowerIndexInclusive);

                    if (upperIndexInclusive > 0 && upperIndexInclusive != Int64.MaxValue)
                    {
                        command.Parameters.AddWithValue("@upperIndexInclusive", upperIndexInclusive);
                    }

                    await PushToSubscriber(command, descending ? upperIndexInclusive : lowerIndexInclusive, subscription, false, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }
        public async Task ReadForwardAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            await ScanRange(
                partitionId: partitionId,
                lowerIndexInclusive: fromLowerIndexInclusive,
                upperIndexInclusive: toUpperIndexInclusive,
                limit: limit,
                @descending: false,
                subscription: subscription,
                cancellationToken: cancellationToken
            ).ConfigureAwait(false);
        }

        public async Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            await ScanRange(
                partitionId: partitionId,
                lowerIndexInclusive: toLowerIndexInclusive,
                upperIndexInclusive: fromUpperIndexInclusive,
                limit: limit,
                @descending: true,
                subscription: subscription,
                cancellationToken: cancellationToken
            ).ConfigureAwait(false);
        }

        private async Task PushToSubscriber(SqlCommand command, long start, ISubscription subscription,
            bool broadcastPosition, CancellationToken cancellationToken)
        {
            long indexOrPosition = 0;
            await subscription.OnStartAsync(start).ConfigureAwait(false);

            try
            {
                using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var chunk = new MsSqlChunk
                        {
                            Position = reader.GetInt64(0),
                            PartitionId = reader.GetString(1),
                            Index = reader.GetInt64(2),
                            OperationId = reader.GetString(4),
                            SerializerInfo = reader.GetString(5),
                        };

                        indexOrPosition = broadcastPosition ? chunk.Position : chunk.Index;

                        // to handle exceptions with correct position
                        chunk.Payload =
                            _options.Serializer.Deserialize((byte[]) reader.GetSqlBinary(3), chunk.SerializerInfo);

                        if (!await subscription.OnNextAsync(chunk).ConfigureAwait(false))
                        {
                            await subscription.StoppedAsync(chunk.Position).ConfigureAwait(false);
                            return;
                        }
                    }
                }

                await subscription.CompletedAsync(indexOrPosition).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await subscription.OnErrorAsync(indexOrPosition, e).ConfigureAwait(false);
            }
        }

        public async Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqlCommand(_options.GetSelectLastChunkSql(), connection))
                {
                    command.Parameters.AddWithValue("@PartitionId", partitionId);
                    command.Parameters.AddWithValue("@toUpperIndexInclusive", fromUpperIndexInclusive);

                    return await ReadSingleChunk(command, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private async Task<MsSqlChunk> ReadSingleChunk(SqlCommand command, CancellationToken cancellationToken)
        {
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!reader.HasRows)
                    return null;

                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                var chunk = new MsSqlChunk
                {
                    Position = reader.GetInt64(0),
                    PartitionId = reader.GetString(1),
                    Index = reader.GetInt64(2),
                    OperationId = reader.GetString(4),
                    SerializerInfo = reader.GetString(5),
                };
                chunk.Payload = _options.Serializer.Deserialize((byte[]) reader.GetSqlBinary(3), chunk.SerializerInfo);
                return chunk;
            }
        }

        public async Task ReadAllAsync(
            long fromPositionInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken)
        {
            var sql = _options.GetReadAllChunksSql(limit);

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@fromPositionInclusive", fromPositionInclusive);

                    await PushToSubscriber(command, fromPositionInclusive, subscription, true, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }

        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            var sql = _options.GetSelectLastPositionSql();

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var command = new SqlCommand(sql, connection))
                {
                    var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    if (result == null)
                        return 0;

                    return (long) result;
                }
            }
        }

        public async Task<IChunk> AppendAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            if (index == -1)
                index = Interlocked.Increment(ref REFACTOR_TO_USE_SEQUENCE_OR_NOT_STRICTLY_SEQUENTIAL_VALUE);

            var chunk = new MsSqlChunk()
            {
                PartitionId = partitionId,
                Index = index,
                Payload = payload,
                OperationId = operationId ?? Guid.NewGuid().ToString()
            };

            var bytes = _options.Serializer.Serialize(payload, out string serializerInfo);
            chunk.SerializerInfo = serializerInfo;

            try
            {
                using (var connection = Connect())
                {
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    using (var command = new SqlCommand(_options.GetInsertChunkSql(), connection))
                    {
                        command.Parameters.AddWithValue("@PartitionId", partitionId);
                        command.Parameters.AddWithValue("@Index", index);
                        command.Parameters.AddWithValue("@OperationId", chunk.OperationId);
                        command.Parameters.AddWithValue("@Payload", new SqlBinary(bytes));
                        command.Parameters.AddWithValue("@SerializerInfo", serializerInfo);

                        chunk.Position =
                            (long) await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (SqlException ex)
            {
                if (ex.Number == DuplicatedIndexExceptionErrorNumber)
                {
                    if (ex.Message.Contains("_IDX"))
                    {
                        throw new DuplicateStreamIndexException(partitionId, index);
                    }

                    if (ex.Message.Contains("_OPID"))
                    {
                        _logger.LogInformation(
                            $"Skipped duplicated chunk on '{partitionId}' by operation id '{operationId}'");
                        return null;
                    }
                }

                _logger.LogError(ex.Message);
                throw;
            }

            return chunk;
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqlCommand(_options.GetDeleteStreamChunksSql(), connection))
                {
                    command.Parameters.AddWithValue("@PartitionId", partitionId);
                    command.Parameters.AddWithValue("@fromLowerIndexInclusive", fromLowerIndexInclusive);
                    command.Parameters.AddWithValue("@toUpperIndexInclusive", toUpperIndexInclusive);

                    var deleted = (long) await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                    if (deleted == 0)
                        throw new StreamDeleteException(partitionId);
                }
            }
        }

        public async Task<IChunk> ReadByOperationIdAsync(
            string partitionId,
            string operationId,
            CancellationToken cancellationToken
        )
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqlCommand(_options.GetSelectChunkByStreamAndOperation(), connection))
                {
                    command.Parameters.AddWithValue("@PartitionId", partitionId);
                    command.Parameters.AddWithValue("@OperationId", operationId);

                    return await ReadSingleChunk(command, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public async Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription,
            CancellationToken cancellationToken)
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var command = new SqlCommand(_options.GetSelectAllChunksByOperationSql(), connection))
                {
                    command.Parameters.AddWithValue("@OperationId", operationId);
                    await PushToSubscriber(command, 0, subscription, true, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public async Task InitAsync(CancellationToken cancellationToken)
        {
            await EnsureTable(_options.StreamsTableName, cancellationToken).ConfigureAwait(false);
        }

        public async Task DestroyAllAsync(CancellationToken cancellationToken)
        {
            using (var conn = Connect())
            {
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                var sql = _options.GetDropTableSql();
                using (var cmd = new SqlCommand(sql, conn))
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private async Task EnsureTable(string tableName, CancellationToken cancellationToken)
        {
            using (var conn = Connect())
            {
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                var sql = _options.GetCreateTableIfMissingSql(tableName);
                using (var cmd = new SqlCommand(sql, conn))
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private SqlConnection Connect()
        {
            return new SqlConnection(_options.ConnectionString);
        }
    }
}