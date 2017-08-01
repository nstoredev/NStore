using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NStore.Persistence.MsSql
{
    public sealed class MsSqlChunk : IChunk
    {
        public long Position { get; set; }
        public string PartitionId { get; set; }
        public long Index { get; set; }
        public object Payload { get; set; }
        public string OperationId { get; set; }
        public bool Deleted { get; set; }
    }

    public interface IMsSqlPayloadSearializer
    {
        string Serialize(object payload);
        object Deserialize(string serialized);
    }

    public class MsSqlPersistenceOptions
    {
        public ILoggerFactory LoggerFactory { get; set; }
        public IMsSqlPayloadSearializer Serializer { get; set; }
        public string ConnectionString { get; set; }
        public string StreamsTableName { get; set; }

        public MsSqlPersistenceOptions(ILoggerFactory loggerFactory)
        {
            LoggerFactory = loggerFactory;
            StreamsTableName = "Streams";
        }

        public virtual string GetCreateTableScript()
        {
            return $@"CREATE TABLE [{StreamsTableName}](
                [Position] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                [PartitionId] NVARCHAR(255) NOT NULL,
                [OperationId] NVARCHAR(255) NOT NULL,
                [Index] BIGINT NOT NULL,
                [Deleted] BIT NOT NULL,
                [Payload] NVARCHAR(MAX)
            )

            CREATE UNIQUE INDEX IX_{StreamsTableName}_OPID on dbo.{StreamsTableName} (PartitionId, OperationId)
            CREATE UNIQUE INDEX IX_{StreamsTableName}_IDX on dbo.{StreamsTableName} (PartitionId, [Index])
";
        }

        public virtual string GetPersistScript()
        {
            return $@"INSERT INTO [{StreamsTableName}]
                      ([PartitionId], [Index], [Payload], [OperationId], [Deleted])
                      OUTPUT INSERTED.[Position] 
                      VALUES (@PartitionId, @Index, @Payload, @OperationId, 0)";
        }

        public virtual string GetDeleteStreamScript()
        {
            return $@"DELETE FROM [{StreamsTableName}] WHERE 
                          [PartitionId] = @PartitionId 
                      AND [Index] BETWEEN @fromLowerIndexInclusive AND @toUpperIndexInclusive";
        }

        public virtual string GetLastChunkScript()
        {
            return $@"SELECT TOP 1 
                        [Position], [PartitionId], [Index], [Payload], [OperationId], [Deleted]
                      FROM 
                        [{StreamsTableName}] 
                      WHERE 
                          [PartitionId] = @PartitionId 
                      AND [Index] <= @toUpperIndexInclusive 
                      ORDER BY 
                          [Position] DESC";
        }
    }

    public class MsSqlPersistence : IPersistence
    {
        private readonly MsSqlPersistenceOptions _options;
        private readonly ILogger _logger;

        public bool SupportsFillers => false;

        public MsSqlPersistence(MsSqlPersistenceOptions options)
        {
            _options = options;
            _logger = _options.LoggerFactory.CreateLogger($"MsSqlPersistence-{options.StreamsTableName}");

            if (_options.Serializer == null)
            {
                throw new Exception("MsSqlOptions should provide a custom Serializer");
            }
        }

        public async Task ReadPartitionForward(
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

            sb.Append("[Position], [PartitionId], [Index], [Payload], [OperationId], [Deleted] ");
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

                    long position = 0;
                    using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            var chunk = new MsSqlChunk
                            {
                                Position = position = reader.GetInt64(0),
                                PartitionId = reader.GetString(1),
                                Index = reader.GetInt64(2),
                                Payload = _options.Serializer.Deserialize(reader.GetString(3)),
                                OperationId = reader.GetString(4),
                                Deleted = reader.GetBoolean(5)
                            };

                            if (!await subscription.OnNext(chunk).ConfigureAwait(false))
                            {
                                await subscription.Completed(chunk.Position).ConfigureAwait(false);
                                return;
                            }
                        }
                    }
                    await subscription.Completed(position).ConfigureAwait(false);
                }
            }
        }

        public async Task ReadPartitionBackward(
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

            sb.Append("[Position], [PartitionId], [Index], [Payload], [OperationId], [Deleted] ");
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

                    long position = 0;
                    using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            var chunk = new MsSqlChunk
                            {
                                Position = position = reader.GetInt64(0),
                                PartitionId = reader.GetString(1),
                                Index = reader.GetInt64(2),
                                Payload = _options.Serializer.Deserialize(reader.GetString(3)),
                                OperationId = reader.GetString(4),
                                Deleted = reader.GetBoolean(5)
                            };

                            if (!await subscription.OnNext(chunk).ConfigureAwait(false))
                            {
                                await subscription.Completed(chunk.Position).ConfigureAwait(false);
                                return;
                            }
                        }
                    }
                    await subscription.Completed(position).ConfigureAwait(false);
                }
            }
        }

        public async Task<IChunk> ReadLast(string partitionId, int toUpperIndexInclusive, CancellationToken cancellationToken)
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqlCommand(_options.GetLastChunkScript(), connection))
                {
                    command.Parameters.AddWithValue("@PartitionId", partitionId);
                    command.Parameters.AddWithValue("@toUpperIndexInclusive", toUpperIndexInclusive);

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
                            Payload = _options.Serializer.Deserialize(reader.GetString(3)),
                            OperationId = reader.GetString(4),
                            Deleted = reader.GetBoolean(5)
                        };

                        return chunk;
                    }
                }
            }
        }

        public async Task ReadAllAsync(
            long fromSequenceIdInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken)
        {
            var top = limit != Int32.MaxValue ? $"TOP {limit}" : "";

            var sql = $@"SELECT {top} 
                        [Position], [PartitionId], [Index], [Payload], [OperationId], [Deleted]
                      FROM 
                        [{_options.StreamsTableName}] 
                      WHERE 
                          [Position] >= @fromPositionInclusive 
                      ORDER BY 
                          [Position]";

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                long position = 0;
                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@fromPositionInclusive", fromSequenceIdInclusive);
                    await subscription.OnStart(fromSequenceIdInclusive).ConfigureAwait(false);
                    using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            var chunk = new MsSqlChunk
                            {
                                Position = position = reader.GetInt64(0),
                                PartitionId = reader.GetString(1),
                                Index = reader.GetInt64(2),
                                Payload = _options.Serializer.Deserialize(reader.GetString(3)),
                                OperationId = reader.GetString(4),
                                Deleted = reader.GetBoolean(5)
                            };

                            if (!await subscription.OnNext(chunk).ConfigureAwait(false))
                            {
                                await subscription.Stopped(position).ConfigureAwait(false);
                                return;
                            }
                        }
                    }
                }

                if (position == 0)
                {
                    await subscription.Stopped(fromSequenceIdInclusive).ConfigureAwait(false);
                }
                else
                {
                    await subscription.Completed(position).ConfigureAwait(false);
                }
            }
        }

        public async Task<IChunk> PersistAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            if (index == -1)
                index = Interlocked.Increment(ref USE_SEQUENCE_INSTEAD);

            //@@TODO remove and pass only Position back to caller
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

                        var position = (long)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                        chunk.Position = position;
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
