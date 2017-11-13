using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Logging;
using NStore.Core.Persistence;

namespace NStore.Persistence.Sqlite
{
    public class SqlitePersistence : IPersistence, IEnhancedPersistence
    {
        private readonly SqlitePersistenceOptions _options;
        private readonly INStoreLogger _logger;

        public bool SupportsFillers => false;

        public SqlitePersistence(SqlitePersistenceOptions options)
        {
            _options = options;
            _logger = _options.LoggerFactory.CreateLogger($"SqlitePersistence-{options.StreamsTableName}");

            if (_options.Serializer == null)
            {
                throw new SqlitePersistenceException("SqliteOptions should provide a custom Serializer");
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


            sb.Append("[Position], [PartitionId], [Index], [Payload], [OperationId] ");
            sb.Append($"FROM {_options.StreamsTableName} ");
            sb.Append("WHERE [PartitionId] = @PartitionId ");

            if (fromLowerIndexInclusive > 0)
                sb.Append("AND [Index] >= @fromLowerIndexInclusive ");

            if (toUpperIndexInclusive > 0 && toUpperIndexInclusive != Int64.MaxValue)
            {
                sb.Append("AND [Index] <= @toUpperIndexInclusive ");
            }

            sb.Append("ORDER BY [Index]");

            if (limit > 0 && limit != int.MaxValue)
            {
                sb.Append($" LIMIT {limit} ");
            }

            var sql = sb.ToString();

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqliteCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@PartitionId", partitionId);

                    if (fromLowerIndexInclusive > 0)
                        command.Parameters.AddWithValue("@fromLowerIndexInclusive", fromLowerIndexInclusive);

                    if (toUpperIndexInclusive > 0 && toUpperIndexInclusive != Int64.MaxValue)
                    {
                        command.Parameters.AddWithValue("@toUpperIndexInclusive", toUpperIndexInclusive);
                    }

                    await PushToSubscriber(command, fromLowerIndexInclusive, subscription, false, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }

        private async Task<IList<SqliteChunk>> Load(SqliteCommand command, CancellationToken cancellationToken)
        {
            var list = new List<SqliteChunk>();
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var chunk = new SqliteChunk
                    {
                        Position = reader.GetInt64(0),
                        PartitionId = reader.GetString(1),
                        Index = reader.GetInt64(2),
                        OperationId = reader.GetString(4),
                        Payload = _options.Serializer.Deserialize(reader.GetString(3))
                    };

                    list.Add(chunk);
                }
            }

            return list;
        }

        private async Task PushToSubscriber(
            SqliteCommand command,
            long start,
            ISubscription subscription,
            bool broadcastPosition,
            CancellationToken cancellationToken)
        {
            long indexOrPosition = 0;
            await subscription.OnStartAsync(start).ConfigureAwait(false);

            try
            {
                using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var chunk = new SqliteChunk
                        {
                            Position = reader.GetInt64(0),
                            PartitionId = reader.GetString(1),
                            Index = reader.GetInt64(2),
                            OperationId = reader.GetString(4)
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

            sb.Append("[Position], [PartitionId], [Index], [Payload], [OperationId] ");
            sb.Append($"FROM {_options.StreamsTableName} ");
            sb.Append("WHERE [PartitionId] = @PartitionId ");

            if (fromUpperIndexInclusive > 0)
            {
                sb.Append("AND [Index] <= @fromUpperIndexInclusive ");
            }

            if (toLowerIndexInclusive > 0 && toLowerIndexInclusive != Int64.MinValue)
            {
                sb.Append("AND [Index] >= @toLowerIndexInclusive ");
            }
            sb.Append("ORDER BY [Index] DESC");

            if (limit > 0 && limit != int.MaxValue)
            {
                sb.Append($" LIMIT {limit} ");
            }

            var sql = sb.ToString();

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqliteCommand(sql, connection))
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

                    await PushToSubscriber(command, fromUpperIndexInclusive, subscription, false, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }

        public async Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqliteCommand(_options.GetLastChunkScript(), connection))
                {
                    command.Parameters.AddWithValue("@PartitionId", partitionId);
                    command.Parameters.AddWithValue("@toUpperIndexInclusive", fromUpperIndexInclusive);

                    using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if (!reader.HasRows)
                            return null;

                        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                        var chunk = new SqliteChunk
                        {
                            Position = reader.GetInt64(0),
                            PartitionId = reader.GetString(1),
                            Index = reader.GetInt64(2),
                            OperationId = reader.GetString(4),
                        };

                        chunk.Payload = _options.Serializer.Deserialize(reader.GetString(3));

                        return chunk;
                    }
                }
            }
        }

        public async Task ReadAllAsync(
            long fromPositionInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken)
        {
            var top = Math.Min(limit, 200);

            var sql = $@"SELECT  
                        [Position], [PartitionId], [Index], [Payload], [OperationId]
                      FROM 
                        [{_options.StreamsTableName}] 
                      WHERE 
                          [Position] >= @fromPositionInclusive 
                      ORDER BY 
                          [Position] LIMIT {top}";

            await subscription.OnStartAsync(fromPositionInclusive).ConfigureAwait(false);
            var lastPosition = fromPositionInclusive;
            int readcount = 0;

            try
            {
                while (true)
                {
                    IList<SqliteChunk> buffer = null;

                    using (var connection = Connect())
                    {
                        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                        using (var command = new SqliteCommand(sql, connection))
                        {
                            command.Parameters.AddWithValue("@fromPositionInclusive", fromPositionInclusive);
                            buffer = await Load(command, cancellationToken).ConfigureAwait(false);
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

        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            var sql = $@"SELECT 
                        [Position]
                      FROM 
                        [{_options.StreamsTableName}] 
                      ORDER BY 
                          [Position] DESC
                      LIMIT 1";

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var command = new SqliteCommand(sql, connection))
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
                index = Interlocked.Increment(ref USE_SEQUENCE_INSTEAD);

            var chunk = new SqliteChunk()
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
                    using (var command =
                        new SqliteCommand(_options.GetPersistScript(_options.StreamsTableName), connection))
                    {
                        command.Parameters.AddWithValue("@PartitionId", partitionId);
                        command.Parameters.AddWithValue("@Index", index);
                        command.Parameters.AddWithValue("@OperationId", chunk.OperationId);
                        command.Parameters.AddWithValue("@Payload", textPayload);

                        chunk.Position =
                            (long) await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (SqliteException ex)
            {
                if (ex.SqliteErrorCode == DUPLICATED_INDEX_EXCEPTION)
                {
                    if (ex.Message.Contains(".PartitionId") && ex.Message.Contains(".Index"))
                    {
                        throw new DuplicateStreamIndexException(partitionId, index);
                    }

                    if (ex.Message.Contains(".PartitionId") && ex.Message.Contains(".OperationId"))
                    {
                        _logger.LogInformation(
                            $"Skipped duplicated chunk on '{partitionId}' by operation id '{operationId}'");
                        return null;
                    }
                }

                Console.WriteLine(_options.Serializer.Serialize(ex));

                _logger.LogError(ex.Message);
                throw;
            }

            return chunk;
        }

        private const int DUPLICATED_INDEX_EXCEPTION = 19;

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

                using (var command = new SqliteCommand(_options.GetDeleteStreamScript(), connection))
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

        private async Task<IChunk> ReadSingleChunk(SqliteCommand command, CancellationToken cancellationToken)
        {
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!reader.HasRows)
                    return null;

                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                var chunk = new SqliteChunk()
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

        public async Task<IChunk> ReadByOperationIdAsync(
            string partitionId,
            string operationId,
            CancellationToken cancellationToken
        )
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = new SqliteCommand(_options.GetFindByStreamAndOperation(), connection))
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
                using (var command = new SqliteCommand(_options.GetFindAllByOperation(), connection))
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
                var sql = $"DROP TABLE IF EXISTS {_options.StreamsTableName} ";
                using (var cmd = new SqliteCommand(sql, conn))
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private async Task EnsureTable(string tableName, CancellationToken cancellationToken)
        {
            var sql = _options.GetCreateTableScript(tableName);

            using (var conn = Connect())
            {
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var cmd = new SqliteCommand(sql, conn))
                {
                    var result = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private SqliteConnection Connect()
        {
            return new SqliteConnection(_options.ConnectionString);
        }

        public async Task AppendBatchAsync(WriteJob[] queue, CancellationToken cancellationToken)
        {
            var chunks = queue.Select(x => new SqliteChunk()
            {
                PartitionId = x.PartitionId,
                Index = x.Index == -1 ? Interlocked.Increment(ref USE_SEQUENCE_INSTEAD) : x.Index,
                OperationId = x.OperationId ?? Guid.NewGuid().ToString(),
                Payload = x.Payload,
                TextPayload = _options.Serializer.Serialize(x.Payload)
            }).ToArray();

            try
            {
                using (var connection = Connect())
                {
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    using (var transaction = connection.BeginTransaction())
                    {
                        for (var index = 0; index < chunks.Length; index++)
                        {
                            var chunk = chunks[index];
                            using (var command = new SqliteCommand(_options.GetPersistScript(_options.StreamsTableName),
                                connection, transaction))
                            {
                                command.Parameters.AddWithValue("@PartitionId", chunk.PartitionId);
                                command.Parameters.AddWithValue("@Index", chunk.Index);
                                command.Parameters.AddWithValue("@OperationId", chunk.OperationId);
                                command.Parameters.AddWithValue("@Payload", chunk.TextPayload);

                                try
                                {
                                    chunk.Position = (long) await command.ExecuteScalarAsync(cancellationToken)
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

                                    Console.WriteLine(_options.Serializer.Serialize(ex));
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
    }

    public sealed class SqliteChunk : IChunk
    {
        public long Position { get; set; }
        public string PartitionId { get; set; }
        public long Index { get; set; }
        public object Payload { get; set; }
        public string OperationId { get; set; }
        public string TextPayload { get; set; }
    }
}