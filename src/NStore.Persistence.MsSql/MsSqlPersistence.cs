using System;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Logging;
using NStore.Core.Persistence;

namespace NStore.Persistence.MsSql
{
    public class MsSqlPersistence : IPersistence
    {
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

        public async Task ReadForwardAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            var sb = new StringBuilder("SELECT ");
            if (limit > 0 && limit != int.MaxValue)
            {
                sb.Append($"TOP {limit} ");
            }

            sb.Append("[Position], [PartitionId], [Index], [Payload], [OperationId] ");
            sb.Append($"FROM {_options.StreamsTableName} ");
            sb.Append($"WHERE [PartitionId] = @PartitionId ");

            if (fromLowerIndexInclusive > 0)
                sb.Append("AND [Index] >= @fromLowerIndexInclusive ");

            if (toUpperIndexInclusive > 0 && toUpperIndexInclusive != Int64.MaxValue)
            {
                sb.Append("AND [Index] <= @toUpperIndexInclusive ");
            }

            sb.Append("ORDER BY [Index]");

            var sql = sb.ToString();

            _logger.LogDebug($"Executing {sql}");

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@PartitionId", partitionId);

                    if (fromLowerIndexInclusive > 0)
                        command.Parameters.AddWithValue("@fromLowerIndexInclusive", fromLowerIndexInclusive);

                    if (toUpperIndexInclusive > 0 && toUpperIndexInclusive != Int64.MaxValue)
                    {
                        command.Parameters.AddWithValue("@toUpperIndexInclusive", toUpperIndexInclusive);
                    }

                    await PushToSubscriber(command, fromLowerIndexInclusive, subscription, false, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private async Task PushToSubscriber(SqlCommand command, long start, ISubscription subscription, bool broadcastPosition, CancellationToken cancellationToken)
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
                        };

                        indexOrPosition = broadcastPosition ? chunk.Position : chunk.Index;

                        // to handle exceptions with correct position
                        chunk.Payload = _options.Serializer.Deserialize(reader.GetString(3));

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

        public async Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            var sb = new StringBuilder("SELECT ");
            if (limit > 0 && limit != int.MaxValue)
            {
                sb.Append($"TOP {limit} ");
            }

            sb.Append("[Position], [PartitionId], [Index], [Payload], [OperationId] ");
            sb.Append($"FROM {_options.StreamsTableName} ");
            sb.Append($"WHERE [PartitionId] = @PartitionId ");

            if (fromUpperIndexInclusive > 0)
            {
                sb.Append("AND [Index] <= @fromUpperIndexInclusive ");
            }

            if (toLowerIndexInclusive > 0 && toLowerIndexInclusive != Int64.MinValue)
            {
                sb.Append("AND [Index] >= @toLowerIndexInclusive ");
            }
            sb.Append("ORDER BY [Index] DESC");

            var sql = sb.ToString();

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@PartitionId", partitionId);
                    if (fromUpperIndexInclusive > 0)
                    {
                        command.Parameters.AddWithValue("@fromUpperIndexInclusive", fromUpperIndexInclusive);
                    }

                    if (toLowerIndexInclusive > 0 && toLowerIndexInclusive != Int64.MinValue)
                    {
                        command.Parameters.AddWithValue("@toLowerIndexInclusive", toLowerIndexInclusive);
                    }

                    await PushToSubscriber(command, fromUpperIndexInclusive, subscription, false, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public async Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive, CancellationToken cancellationToken)
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqlCommand(_options.GetLastChunkScript(), connection))
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
                    Payload = _options.Serializer.Deserialize(reader.GetString(3)),
                };
                return chunk;
            }
        }

        public async Task ReadAllAsync(
            long fromPositionInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken)
        {
            var top = limit != Int32.MaxValue ? $"TOP {limit}" : "";

            var sql = $@"SELECT {top} 
                        [Position], [PartitionId], [Index], [Payload], [OperationId]
                      FROM 
                        [{_options.StreamsTableName}] 
                      WHERE 
                          [Position] >= @fromPositionInclusive 
                      ORDER BY 
                          [Position]";

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@fromPositionInclusive", fromPositionInclusive);

                    await PushToSubscriber(command, fromPositionInclusive, subscription, true, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            var sql = $@"SELECT TOP 1
                        [Position]
                      FROM 
                        [{_options.StreamsTableName}] 
                      ORDER BY 
                          [Position] DESC";

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var command = new SqlCommand(sql, connection))
                {
                    var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    if (result == null)
                        return 0;

                    return (long)result;
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
                index = Interlocked.Increment(ref USE_SEQUENCE_INSTEAD);

            var chunk = new MsSqlChunk()
            {
                PartitionId = partitionId,
                Index = index,
                Payload = payload,
                OperationId = operationId ?? Guid.NewGuid().ToString()
            };

            string textPayload = _options.Serializer.Serialize(payload);

            try
            {
                using (var connection = Connect())
                {
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    using (var command = new SqlCommand(_options.GetPersistScript(), connection))
                    {
                        command.Parameters.AddWithValue("@PartitionId", partitionId);
                        command.Parameters.AddWithValue("@Index", index);
                        command.Parameters.AddWithValue("@OperationId", chunk.OperationId);
                        command.Parameters.AddWithValue("@Payload", textPayload);

                        chunk.Position = (long)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (SqlException ex)
            {
                if (ex.Number == DUPLICATED_INDEX_EXCEPTION)
                {
                    if (ex.Message.Contains("_IDX"))
                    {
                        throw new DuplicateStreamIndexException(partitionId, index);
                    }

                    if (ex.Message.Contains("_OPID"))
                    {
                        _logger.LogInformation($"Skipped duplicated chunk on '{partitionId}' by operation id '{operationId}'");
                        return null;
                    }
                }

                _logger.LogError(ex.Message);
                throw;
            }

            return chunk;
        }

        private const int DUPLICATED_INDEX_EXCEPTION = 2601;

        private int USE_SEQUENCE_INSTEAD = 0;

        public async Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqlCommand(_options.GetDeleteStreamScript(), connection))
                {
                    command.Parameters.AddWithValue("@PartitionId", partitionId);
                    command.Parameters.AddWithValue("@fromLowerIndexInclusive", fromLowerIndexInclusive);
                    command.Parameters.AddWithValue("@toUpperIndexInclusive", toUpperIndexInclusive);

                    var deleted = (long)await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

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

                using (var command = new SqlCommand(_options.GetFindByStreamAndOperation(), connection))
                {
                    command.Parameters.AddWithValue("@PartitionId", partitionId);
                    command.Parameters.AddWithValue("@OperationId", operationId);

                    return await ReadSingleChunk(command, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public async Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription, CancellationToken cancellationToken)
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var command = new SqlCommand(_options.GetFindAllByOperation(), connection))
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
                var sql = $"if exists (select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME = '{_options.StreamsTableName}' AND TABLE_SCHEMA = 'dbo') " +
                          $"DROP TABLE {_options.StreamsTableName}";
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
                var sql = GetCreateTableIfMissingSql(tableName, _options.GetCreateTableScript());
                using (var cmd = new SqlCommand(sql, conn))
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private string GetCreateTableIfMissingSql(string tableName, string sql)
        {
            return $@"
if not exists (select * from dbo.sysobjects where id = object_id(N'{tableName}') and OBJECTPROPERTY(id, N'IsUserTable') = 1) 
BEGIN
{sql}
END
";
        }

        private SqlConnection Connect()
        {
            return new SqlConnection(_options.ConnectionString);
        }
    }
}
