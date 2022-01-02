using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Logging;
using NStore.Core.Persistence;

namespace NStore.BaseSqlPersistence
{
    public abstract class AbstractSqlPersistence
    {
        private readonly INStoreLogger _logger;

        private BaseSqlPersistenceOptions Options { get; }

        protected AbstractSqlPersistence(BaseSqlPersistenceOptions options)
        {
            Options = options;
            _logger = options.LoggerFactory.CreateLogger($"{GetType().FullName}-{options.StreamsTableName}");
        }

        protected abstract bool IsDuplicatedStreamOperation(Exception exception);
        protected abstract bool IsDuplicatedStreamIndex(Exception exception);


        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            var sql = Options.GetSelectLastPositionSql();

            using (var context = await Options.GetContextAsync(cancellationToken).ConfigureAwait(false))
            {
                using (var command = context.CreateCommand(sql))
                {
                    var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    if (result == null)
                        return 0;

                    return (long) result;
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
                OperationId = reader.IsDBNull(4) ? null : reader.GetString(4),
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
            using (var context = await Options.GetContextAsync(cancellationToken).ConfigureAwait(false))
            {
                using (var command = context.CreateCommand(Options.GetSelectLastChunkSql()))
                {
                    context.AddParam(command, "@PartitionId", partitionId);
                    context.AddParam(command, "@toUpperIndexInclusive", fromUpperIndexInclusive);

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
            using (var context = await Options.GetContextAsync(cancellationToken).ConfigureAwait(false))
            {
                using (var command = context.CreateCommand(Options.GetDeleteStreamChunksSql()))
                {
                    context.AddParam(command, "@PartitionId", partitionId);
                    context.AddParam(command, "@fromLowerIndexInclusive", fromLowerIndexInclusive);
                    context.AddParam(command, "@toUpperIndexInclusive", toUpperIndexInclusive);

                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public async Task<IChunk> ReadByOperationIdAsync(
            string partitionId,
            string operationId,
            CancellationToken cancellationToken
        )
        {
            using (var context = await Options.GetContextAsync(cancellationToken).ConfigureAwait(false))
            {
                using (var command = context.CreateCommand(Options.GetSelectChunkByStreamAndOperation()))
                {
                    context.AddParam(command, "@PartitionId", partitionId);
                    context.AddParam(command, "@OperationId", operationId);

                    return await ReadSingleChunk(command, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public async Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription,
            CancellationToken cancellationToken)
        {
            using (var context = await Options.GetContextAsync(cancellationToken).ConfigureAwait(false))
            {
                using (var command = context.CreateCommand(Options.GetSelectAllChunksByOperationSql()))
                {
                    context.AddParam(command, "@OperationId", operationId);
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
            if (index < 0)
            {
                throw new InvalidStreamIndexException(partitionId, index);
            }

            try
            {
                var chunk = new SqlChunk()
                {
                    PartitionId = partitionId,
                    Index = index,
                    Payload = payload,
                    OperationId = operationId
                };

                if (chunk.OperationId == null && Options.StreamIdempotencyEnabled)
                {
                    chunk.OperationId = Guid.NewGuid().ToString();
                }

                var bytes = Options.Serializer.Serialize(payload, out string serializerInfo);
                chunk.SerializerInfo = serializerInfo;
                var sql = Options.GetInsertChunkSql();

                using (var context = await Options.GetContextAsync(cancellationToken).ConfigureAwait(false))
                {
                    using (var command = context.CreateCommand(sql))
                    {
                        context.AddParam(command, "@PartitionId", partitionId);
                        context.AddParam(command, "@Index", index);
                        if (chunk.OperationId == null)
                        {
                            context.AddParam(command, "@OperationId", DBNull.Value);
                        }
                        else
                        {
                            context.AddParam(command, "@OperationId", chunk.OperationId);
                        }
                        context.AddParam(command, "@Payload", bytes);
                        context.AddParam(command, "@SerializerInfo", serializerInfo);

                        chunk.Position =
                            (long) await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
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
                    _logger.LogInformation(
                        $"Skipped duplicated chunk on '{partitionId}' by operation id '{operationId}'");
                    return null;
                }

                _logger.LogError(ex.Message);
                throw;
            }
        } 
        
        public async Task<IChunk> ReplaceAsync(
            long position,
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            if (index < 0)
            {
                throw new InvalidStreamIndexException(partitionId, index);
            }

            try
            {
                var chunk = new SqlChunk()
                {
                    Position = position,
                    PartitionId = partitionId,
                    Index = index,
                    Payload = payload,
                    OperationId = operationId
                };

                if (chunk.OperationId == null && Options.StreamIdempotencyEnabled)
                {
                    chunk.OperationId = Guid.NewGuid().ToString();
                }

                var bytes = Options.Serializer.Serialize(payload, out string serializerInfo);
                chunk.SerializerInfo = serializerInfo;
                var sql = Options.GetRewriteChunkSql();

                using (var context = await Options.GetContextAsync(cancellationToken).ConfigureAwait(false))
                {
                    using (var command = context.CreateCommand(sql))
                    {
                        context.AddParam(command, "@Position", position);
                        context.AddParam(command, "@PartitionId", partitionId);
                        context.AddParam(command, "@Index", index);
                        if (chunk.OperationId == null)
                        {
                            context.AddParam(command, "@OperationId", DBNull.Value);
                        }
                        else
                        {
                            context.AddParam(command, "@OperationId", chunk.OperationId);
                        }
                        context.AddParam(command, "@Payload", bytes);
                        context.AddParam(command, "@SerializerInfo", serializerInfo);

                        await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
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
                    _logger.LogInformation(
                        $"Skipped duplicated chunk on '{partitionId}' by operation id '{operationId}'");
                    return null;
                }

                _logger.LogError(ex.Message);
                throw;
            }
        }

        protected async Task EnsureTable(CancellationToken cancellationToken)
        {
            var sql = Options.GetCreateTableIfMissingSql();

            using (var context = await Options.GetContextAsync(cancellationToken).ConfigureAwait(false))
            {
                using (var cmd = context.CreateCommand(sql))
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
                @descending: descending
            );

            using (var context = await Options.GetContextAsync(cancellationToken).ConfigureAwait(false))
            {
                using (var command = context.CreateCommand(sql))
                {
                    context.AddParam(command, "@PartitionId", partitionId);

                    if (lowerIndexInclusive > 0 && lowerIndexInclusive != Int64.MaxValue)
                    {
                        context.AddParam(command, "@lowerIndexInclusive", lowerIndexInclusive);
                    }

                    if (upperIndexInclusive > 0 && upperIndexInclusive != Int64.MaxValue)
                    {
                        context.AddParam(command, "@upperIndexInclusive", upperIndexInclusive);
                    }

                    await PushToSubscriber(command, descending ? upperIndexInclusive : lowerIndexInclusive,
                            subscription, false, cancellationToken)
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