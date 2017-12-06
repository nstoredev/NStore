using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Logging;
using NStore.Core.Persistence;

namespace NStore.BaseSqlPersistence
{
    public interface ISqlPayloadSearializer
    {
        byte[] Serialize(object payload, out string serializerInfo);
        object Deserialize(byte[] serialized, string serializerInfo);
    }

    public abstract class BaseSqlPersistenceOptions
    {
        public INStoreLoggerFactory LoggerFactory { get; set; }
        public string ConnectionString { get; set; }
        public string StreamsTableName { get; set; }
        public ISqlPayloadSearializer Serializer { get; set; }
        protected BaseSqlPersistenceOptions(INStoreLoggerFactory loggerFactory)
        {
            LoggerFactory = loggerFactory;
            StreamsTableName = "Streams";
        }

        public abstract string GetSelectLastPositionSql();
        public abstract string GetSelectLastChunkSql();
        public abstract string GetDeleteStreamChunksSql();
        public abstract string GetSelectChunkByStreamAndOperation();
        public abstract string GetSelectAllChunksByOperationSql();
        public abstract string GetInsertChunkSql();
        public abstract string GetCreateTableIfMissingSql();
        public abstract string GetReadAllChunksSql(int limit);

        public abstract string GetRangeSelectChunksSql(
            long upperIndexInclusive,
            long lowerIndexInclusive,
            int limit,
            bool descending
        );
    }

    public class SqlChunk : IChunk
    {
        public long Position { get; set; }
        public string PartitionId { get; set; }
        public long Index { get; set; }
        public string OperationId { get; set; }
        public string SerializerInfo { get; set; }
        public object Payload { get; set; }
    }

    public abstract class AbstractSqlPersistence
    {
        private readonly INStoreLogger _logger;

        private BaseSqlPersistenceOptions Options { get; }

        protected AbstractSqlPersistence(BaseSqlPersistenceOptions options)
        {
            Options = options;
            _logger = options.LoggerFactory.CreateLogger($"{GetType().FullName}-{options.StreamsTableName}");
        }

        protected abstract DbConnection Connect();
        protected abstract DbCommand CreateCommand(string sql, DbConnection connection);
        protected abstract DbCommand CreateCommand(string sql, DbConnection connection, DbTransaction transaction);
        protected abstract void AddParam(DbCommand command, string paramName, object value);
        protected abstract long GenerateIndex();
        protected abstract bool IsDuplicatedStreamOperation(Exception exception);
        protected abstract bool IsDuplicatedStreamIndex(Exception exception);


        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            var sql = Options.GetSelectLastPositionSql();

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var command = CreateCommand(sql, connection))
                {
                    var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    if (result == null)
                        return 0;

                    return (long)result;
                }
            }
        }

        protected async Task<IChunk> ReadSingleChunk(DbCommand command, CancellationToken cancellationToken)
        {
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!reader.HasRows)
                    return null;

                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                return ReadChunk(reader);
            }
        }

        protected IChunk ReadChunk(DbDataReader reader)
        {
            var chunk = new SqlChunk
            {
                Position = reader.GetInt64(0),
                PartitionId = reader.GetString(1),
                Index = reader.GetInt64(2),
                OperationId = reader.GetString(4),
                SerializerInfo = reader.GetString(5),
            };

            chunk.Payload = Options.Serializer.Deserialize
            (
                reader.GetFieldValue<byte[]>(3),
                chunk.SerializerInfo
            );

            return chunk;
        }

        public async Task<IChunk> ReadSingleBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = CreateCommand(Options.GetSelectLastChunkSql(), connection))
                {
                    AddParam(command, "@PartitionId", partitionId);
                    AddParam(command, "@toUpperIndexInclusive", fromUpperIndexInclusive);

                    return await ReadSingleChunk(command, cancellationToken).ConfigureAwait(false);
                }
            }
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

                using (var command = CreateCommand(Options.GetDeleteStreamChunksSql(), connection))
                {
                    AddParam(command, "@PartitionId", partitionId);
                    AddParam(command, "@fromLowerIndexInclusive", fromLowerIndexInclusive);
                    AddParam(command, "@toUpperIndexInclusive", toUpperIndexInclusive);

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

                using (var command = CreateCommand(Options.GetSelectChunkByStreamAndOperation(), connection))
                {
                    AddParam(command, "@PartitionId", partitionId);
                    AddParam(command, "@OperationId", operationId);

                    return await ReadSingleChunk(command, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public async Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription, CancellationToken cancellationToken)
        {
            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var command = CreateCommand(Options.GetSelectAllChunksByOperationSql(), connection))
                {
                    AddParam(command, "@OperationId", operationId);
                    await PushToSubscriber(command, 0, subscription, true, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        protected async Task PushToSubscriber(
            DbCommand command,
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
                        var chunk = ReadChunk(reader);
                        indexOrPosition = broadcastPosition ? chunk.Position : chunk.Index;

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

        public async Task<IChunk> AppendAsync(
          string partitionId,
          long index,
          object payload,
          string operationId,
          CancellationToken cancellationToken)
        {
            try
            {
                if (index == -1)
                {
                    index = GenerateIndex();
                }

                var chunk = new SqlChunk()
                {
                    PartitionId = partitionId,
                    Index = index,
                    Payload = payload,
                    OperationId = operationId ?? Guid.NewGuid().ToString()
                };

                var bytes = Options.Serializer.Serialize(payload, out string serializerInfo);
                chunk.SerializerInfo = serializerInfo;
                var sql = Options.GetInsertChunkSql();

                using (var connection = Connect())
                {
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    using (var command = CreateCommand(sql, connection))
                    {
                        AddParam(command, "@PartitionId", partitionId);
                        AddParam(command, "@Index", index);
                        AddParam(command, "@OperationId", chunk.OperationId);
                        AddParam(command, "@Payload", bytes);
                        AddParam(command, "@SerializerInfo", serializerInfo);

                        chunk.Position =
                            (long)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    }
                }

                return chunk;
            }
            catch (Exception ex)
            {
                if (IsDuplicatedStreamIndex(ex))
                {
                    throw new DuplicateStreamIndexException(partitionId, index);
                }

                if (IsDuplicatedStreamOperation(ex))
                {
                    _logger.LogInformation($"Skipped duplicated chunk on '{partitionId}' by operation id '{operationId}'");
                    return null;
                }

                _logger.LogError(ex.Message);
                throw;
            }
        }

        protected async Task EnsureTable(CancellationToken cancellationToken)
        {
            var sql = Options.GetCreateTableIfMissingSql();

            using (var conn = Connect())
            {
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var cmd = CreateCommand(sql, conn))
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        protected async Task ScanRange(
            string partitionId,
            long lowerIndexInclusive,
            long upperIndexInclusive,
            int limit,
            bool descending,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            var sql = Options.GetRangeSelectChunksSql(
                upperIndexInclusive: upperIndexInclusive,
                lowerIndexInclusive: lowerIndexInclusive,
                limit: limit,
                descending: descending
            );

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using (var command = CreateCommand(sql, connection))
                {
                    AddParam(command, "@PartitionId", partitionId);

                    if (lowerIndexInclusive > 0 && lowerIndexInclusive != Int64.MaxValue)
                        AddParam(command, "@lowerIndexInclusive", lowerIndexInclusive);

                    if (upperIndexInclusive > 0 && upperIndexInclusive != Int64.MaxValue)
                    {
                        AddParam(command, "@upperIndexInclusive", upperIndexInclusive);
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

    }
}
