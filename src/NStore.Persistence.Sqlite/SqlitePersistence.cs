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
    public class SqlitePersistence : AbstractSqlPersistence, IPersistence, IEnhancedPersistence
    {
        private const int DUPLICATED_INDEX_EXCEPTION = 19;
        private int REFACTOR_TO_USE_SEQUENCE_OR_NOT_STRICTLY_SEQUENTIAL_VALUE = 0;

        private readonly SqlitePersistenceOptions _options;
        private readonly INStoreLogger _logger;


        public bool SupportsFillers => false;

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

                    using (var connection = Connect())
                    {
                        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                        using (var command = CreateCommand(sql, connection))
                        {
                            AddParam(command, "@fromPositionInclusive", fromPositionInclusive);
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

        protected override long GenerateIndex()
        {
            return Interlocked.Increment(ref REFACTOR_TO_USE_SEQUENCE_OR_NOT_STRICTLY_SEQUENTIAL_VALUE);
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

        public async Task InitAsync(CancellationToken cancellationToken)
        {
            await EnsureTable(cancellationToken).ConfigureAwait(false);
        }

        public async Task DestroyAllAsync(CancellationToken cancellationToken)
        {
            using (var conn = Connect())
            {
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                var sql = $"DROP TABLE IF EXISTS {_options.StreamsTableName} ";
                using (var cmd = CreateCommand(sql, conn))
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        protected override DbConnection Connect()
        {
            return new SqliteConnection(_options.ConnectionString);
        }

        protected override DbCommand CreateCommand(string sql, DbConnection connection)
        {
            return new SqliteCommand(sql, (SqliteConnection)connection);
        }

        protected override DbCommand CreateCommand(string sql, DbConnection connection, DbTransaction transaction)
        {
            return new SqliteCommand(sql, (SqliteConnection)connection, (SqliteTransaction)transaction);
        }

        public async Task AppendBatchAsync(WriteJob[] queue, CancellationToken cancellationToken)
        {
            var chunks = queue.Select(x =>
            {
                var chunk = new SqlChunk()
                {
                    PartitionId = x.PartitionId,
                    Index = x.Index == -1 ? GenerateIndex() : x.Index,
                    OperationId = x.OperationId ?? Guid.NewGuid().ToString()
                };

                chunk.Payload = _options.Serializer.Serialize(x.Payload, out string serializerInfo);
                chunk.SerializerInfo = serializerInfo;

                return chunk;
            }).ToArray();

            try
            {
                var sql = _options.GetInsertChunkSql();
                using (var connection = Connect())
                {
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    using (var transaction = connection.BeginTransaction())
                    {
                        for (var index = 0; index < chunks.Length; index++)
                        {
                            var chunk = chunks[index];
                            using (var command = CreateCommand(sql, connection, transaction))
                            {
                                AddParam(command, "@PartitionId", chunk.PartitionId);
                                AddParam(command, "@Index", chunk.Index);
                                AddParam(command, "@OperationId", chunk.OperationId);
                                AddParam(command, "@Payload", chunk.Payload);
                                AddParam(command, "@SerializerInfo", chunk.SerializerInfo);

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

        protected override void AddParam(DbCommand command, string paramName, object value)
        {
            if (value is byte[] bytes)
            {
                ((SqliteCommand)command).Parameters.Add(paramName, SqliteType.Blob).Value = bytes;
            }
            else
            {
                ((SqliteCommand)command).Parameters.AddWithValue(paramName, value);
            }
        }
    }
}