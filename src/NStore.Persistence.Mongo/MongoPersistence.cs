using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace NStore.Persistence.Mongo
{
    public interface IMongoPersistence : IPersistence
    {
        Task InitAsync(CancellationToken cancellationToken);
    }

    public class MongoPersistence : MongoPersistence<MongoChunk>
    {
        public MongoPersistence(MongoPersistenceOptions options) : base(options)
        {
        }
    }

    public class MongoPersistence<TChunk> : IMongoPersistence where TChunk : IMongoChunk, new()
    {
        private IMongoDatabase _partitionsDb;
        private IMongoDatabase _countersDb;

        private IMongoCollection<TChunk> _chunks;
        private IMongoCollection<Counter> _counters;
        private readonly ISerializer _serializer;
        private readonly MongoPersistenceOptions _options;

        private long _sequence = 0;

        private const string SequenceIdx = "partition_sequence";
        private const string OperationIdx = "partition_operation";

        public bool SupportsFillers => true;

        public MongoPersistence(MongoPersistenceOptions options)
        {
            if (options == null || !options.IsValid())
                throw new MongoPersistenceException("Invalid options");

            _options = options;
            _serializer = options.Serializer ?? new TypeSystemSerializer();
            Connect();
        }

        private void Connect()
        {
            var partitionsBuild = new MongoUrlBuilder(_options.PartitionsConnectionString);
            _options.CustomizePartitionSettings(partitionsBuild);

            var partitionsClient = new MongoClient(partitionsBuild.ToMongoUrl());

            this._partitionsDb = partitionsClient.GetDatabase(partitionsBuild.DatabaseName);

            if (_options.SequenceConnectionString == null)
            {
                this._countersDb = _partitionsDb;
            }
            else
            {
                var countersUrlBuilder = new MongoUrlBuilder(_options.SequenceConnectionString);
                _options.CustomizeSquenceSettings(countersUrlBuilder);

                var countersClient = new MongoClient(countersUrlBuilder.ToMongoUrl());
                this._countersDb = countersClient.GetDatabase(countersUrlBuilder.DatabaseName);
            }
        }

        public async Task Drop(CancellationToken cancellationToken)
        {
            await this._partitionsDb
                .DropCollectionAsync(_options.PartitionsCollectionName, cancellationToken)
                .ConfigureAwait(false);

            await this._countersDb
                .DropCollectionAsync(_options.SequenceCollectionName, cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task ReadForwardAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Gte(x => x.Index, fromLowerIndexInclusive),
                Builders<TChunk>.Filter.Lte(x => x.Index, toUpperIndexInclusive)
            );

            var sort = Builders<TChunk>.Sort.Ascending(x => x.Index);
            var options = new FindOptions<TChunk>() { Sort = sort };
            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }

            await PushToSubscriber(
                fromLowerIndexInclusive,
                subscription,
                options,
                filter,
                false, cancellationToken).ConfigureAwait(false);
        }

        private async Task PushToSubscriber(long start, ISubscription subscription, FindOptions<TChunk> options, FilterDefinition<TChunk> filter, bool broadcastPosition, CancellationToken cancellationToken)
        {
            long positionOrIndex = 0;
            await subscription.OnStartAsync(start).ConfigureAwait(false);

            try
            {
                using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
                {
                    while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var batch = cursor.Current;
                        foreach (var b in batch)
                        {
                            positionOrIndex = broadcastPosition ? b.Position : b.Index;
                            b.ReplacePayload(_serializer.Deserialize(b.PartitionId, b.Payload));
                            if (!await subscription.OnNextAsync(b).ConfigureAwait(false))
                            {
                                await subscription.StoppedAsync(positionOrIndex).ConfigureAwait(false);
                                return;
                            }
                        }
                    }
                }

                await subscription.CompletedAsync(positionOrIndex).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await subscription.OnErrorAsync(positionOrIndex, e).ConfigureAwait(false);
            }
        }

        public async Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Lte(x => x.Index, fromUpperIndexInclusive),
                Builders<TChunk>.Filter.Gte(x => x.Index, toLowerIndexInclusive)
            );

            var sort = Builders<TChunk>.Sort.Descending(x => x.Index);
            var options = new FindOptions<TChunk>() { Sort = sort };
            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }

            await PushToSubscriber(
                fromUpperIndexInclusive,
                subscription,
                options,
                filter,
                false,
                cancellationToken
            ).ConfigureAwait(false);
        }

        public async Task<IChunk> ReadSingleBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            CancellationToken cancellationToken
        )
        {
            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Lte(x => x.Index, fromUpperIndexInclusive)
            );

            var sort = Builders<TChunk>.Sort.Descending(x => x.Index);
            var options = new FindOptions<TChunk>() { Sort = sort, Limit = 1 };

            using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
            {
                return await cursor.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task ReadAllAsync(long fromPositionInclusive, ISubscription subscription, int limit, CancellationToken cancellationToken)
        {
            var filter = Builders<TChunk>.Filter.Gte(x => x.Position, fromPositionInclusive);

            var options = new FindOptions<TChunk>()
            {
                Sort = Builders<TChunk>.Sort.Ascending(x => x.Position)
            };

            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }

            await PushToSubscriber(fromPositionInclusive, subscription, options, filter, true, cancellationToken).ConfigureAwait(false);
        }

        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            var filter = Builders<TChunk>.Filter.Empty;
            var projection = Builders<TChunk>.Projection.Include(x => x.Position);

            var options = new FindOptions<TChunk, BsonDocument>()
            {
                Sort = Builders<TChunk>.Sort.Descending(x => x.Position),
                Limit = 1,
                Projection = projection
            };

            using (var cursor = await _chunks
                   .FindAsync(filter, options, cancellationToken)
                   .ConfigureAwait(false)
            )
            {
                var lastPosition = await cursor.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                if (lastPosition != null)
                {
                    return lastPosition[0].AsInt64;
                }
                return 0;
            }
        }

        public async Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId, CancellationToken cancellationToken)
        {
            long id = await GetNextId(cancellationToken).ConfigureAwait(false);
            var chunk = new TChunk();
            chunk.Init(
                id,
                partitionId,
                index < 0 ? id : index,
                _serializer.Serialize(partitionId, payload),
                operationId ?? Guid.NewGuid().ToString()
            );
            await InternalPersistAsync(chunk, cancellationToken).ConfigureAwait(false);

            return chunk;
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken
        )
        {
            var filterById = Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId);
            if (fromLowerIndexInclusive > 0)
            {
                filterById = Builders<TChunk>.Filter.And(
                    filterById,
                    Builders<TChunk>.Filter.Gte(x => x.Index, fromLowerIndexInclusive)
                );
            }

            if (toUpperIndexInclusive < long.MaxValue)
            {
                filterById = Builders<TChunk>.Filter.And(
                    filterById,
                    Builders<TChunk>.Filter.Lte(x => x.Index, toUpperIndexInclusive)
                );
            }

            var result = await _chunks.DeleteManyAsync(filterById, cancellationToken).ConfigureAwait(false);
            if (!result.IsAcknowledged || result.DeletedCount == 0)
                throw new StreamDeleteException(partitionId);
        }

        private async Task PersistAsEmptyAsync(
            TChunk chunk,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            TChunk empty;
            //@@REVIEW partial index on mongo?
            if (chunk.PartitionId == "::empty")
            {
                // reuse chunk
                empty = chunk;
                empty.RewriteIndex(empty.Position);
            }
            else
            {
                empty = new TChunk();
                empty.Init(
                    chunk.Position,
                    "::empty",
                    chunk.Position,
                    null,
                    "_" + chunk.Position
                );
            }
            await InternalPersistAsync(empty, cancellationToken).ConfigureAwait(false);
        }

        private async Task InternalPersistAsync(
            TChunk chunk,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            while (true)
            {
                try
                {
                    await _chunks.InsertOneAsync(chunk, cancellationToken: cancellationToken).ConfigureAwait(false);
                    return;
                }
                catch (MongoWriteException ex)
                {
                    if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                    {
                        if (ex.Message.Contains(SequenceIdx))
                        {
                            await PersistAsEmptyAsync(chunk, cancellationToken).ConfigureAwait(false);
                            throw new DuplicateStreamIndexException(chunk.PartitionId, chunk.Index);
                        }

                        if (ex.Message.Contains(OperationIdx))
                        {
                            await PersistAsEmptyAsync(chunk, cancellationToken).ConfigureAwait(false);
                            return;
                        }

                        if (ex.Message.Contains("_id_"))
                        {
                            Console.WriteLine(
                                $"Error writing chunk #{chunk.Position} => {ex.Message} - {ex.GetType().FullName} ");
                            await ReloadSequence(cancellationToken).ConfigureAwait(false);
                            chunk.RewritePosition(await GetNextId(cancellationToken).ConfigureAwait(false));
                            continue;
                        }
                    }

                    throw;
                }
            }
        }

        public async Task InitAsync(CancellationToken cancellationToken)
        {
            if (_partitionsDb == null)
                Connect();

            if (_options.DropOnInit)
                Drop(cancellationToken).Wait(cancellationToken);

            _chunks = _partitionsDb.GetCollection<TChunk>(_options.PartitionsCollectionName);
            _counters = _countersDb.GetCollection<Counter>(_options.SequenceCollectionName);

            await _chunks.Indexes.CreateOneAsync(
                    Builders<TChunk>.IndexKeys
                        .Ascending(x => x.PartitionId)
                        .Ascending(x => x.Index),
                    new CreateIndexOptions()
                    {
                        Unique = true,
                        Name = SequenceIdx
                    }, cancellationToken)
                .ConfigureAwait(false);

            await _chunks.Indexes.CreateOneAsync(
                    Builders<TChunk>.IndexKeys
                        .Ascending(x => x.PartitionId)
                        .Ascending(x => x.OperationId),
                    new CreateIndexOptions()
                    {
                        Unique = true,
                        Name = OperationIdx
                    }, cancellationToken)
                .ConfigureAwait(false);

            if (_options.UseLocalSequence)
            {
                await ReloadSequence(cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task ReloadSequence(CancellationToken cancellationToken = default(CancellationToken))
        {
            var filter = Builders<TChunk>.Filter.Empty;
            var lastSequenceNumber = await _chunks
                .Find(filter)
                .SortByDescending(x => x.Position)
                .Project(x => x.Position)
                .Limit(1)
                .FirstOrDefaultAsync(cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            this._sequence = lastSequenceNumber;
        }

        private async Task<long> GetNextId(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_options.UseLocalSequence)
                return Interlocked.Increment(ref _sequence);

            // server side sequence
            var filter = Builders<Counter>.Filter.Eq(x => x.Id, _options.SequenceId);
            var update = Builders<Counter>.Update.Inc(x => x.LastValue, 1);
            var options = new FindOneAndUpdateOptions<Counter>()
            {
                IsUpsert = true,
                ReturnDocument = ReturnDocument.After
            };

            var updateResult = await _counters.FindOneAndUpdateAsync(
                    filter,
                    update,
                    options,
                    cancellationToken
                )
                .ConfigureAwait(false);

            return updateResult.LastValue;
        }
    }
}