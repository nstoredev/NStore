using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace NStore.Persistence.Mongo
{
    public class MongoPersistence : IPersistence
    {
        private IMongoDatabase _partitionsDb;
        private IMongoDatabase _countersDb;

        private IMongoCollection<Chunk> _chunks;
        private IMongoCollection<Counter> _counters;
        private readonly ISerializer _serializer;
        private readonly MongoStoreOptions _options;

        private long _sequence = 0;

        private const string SequenceIdx = "partition_sequence";
        private const string OperationIdx = "partition_operation";

        public bool SupportsFillers => true;

        public MongoPersistence(MongoStoreOptions options)
        {
            if (options == null || !options.IsValid())
                throw new Exception("Invalid options");

            _options = options;
            _serializer = options.Serializer ?? new TypeSystemSerializer();
            Connect();
        }

        private void Connect()
        {
            var partitionsUrl = new MongoUrl(_options.PartitionsConnectionString);
            var partitionSettings = new MongoClientSettings()
            {
                Server = partitionsUrl.Server
            };
            _options.CustomizePartitionSettings(partitionSettings);

            var partitionsClient = new MongoClient(partitionSettings);

            this._partitionsDb = partitionsClient.GetDatabase(partitionsUrl.DatabaseName);

            if (_options.SequenceConnectionString == null)
            {
                this._countersDb = _partitionsDb;
            }
            else
            {
                var countersUrl = new MongoUrl(_options.SequenceConnectionString);
                var countersSettings = new MongoClientSettings()
                {
                    Server = countersUrl.Server
                };
                _options.CustomizeSquenceSettings(countersSettings);

                var countersClient = new MongoClient(countersSettings);
                this._countersDb = countersClient.GetDatabase(countersUrl.DatabaseName);
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

        public async Task ReadPartitionForward(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            var filter = Builders<Chunk>.Filter.And(
                Builders<Chunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<Chunk>.Filter.Gte(x => x.Index, fromLowerIndexInclusive),
                Builders<Chunk>.Filter.Lte(x => x.Index, toUpperIndexInclusive)
            );

            var sort = Builders<Chunk>.Sort.Ascending(x => x.Index);
            var options = new FindOptions<Chunk>() { Sort = sort };
            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }

            await PushToSubscriber(fromLowerIndexInclusive, subscription, options, filter, cancellationToken).ConfigureAwait(false);
        }

        private async Task PushToSubscriber(long start, ISubscription subscription, FindOptions<Chunk> options, FilterDefinition<Chunk> filter, CancellationToken cancellationToken)
        {
            long position = 0;
            await subscription.OnStart(start).ConfigureAwait(false);

            using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
            {
                while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    var batch = cursor.Current;
                    foreach (var b in batch)
                    {
                        position = b.Position;
                        b.Payload = _serializer.Deserialize(b.PartitionId, b.Payload);
                        if (!await subscription.OnNext(b).ConfigureAwait(false))
                        {
                            await subscription.Stopped(position).ConfigureAwait(false);
                            return;
                        }
                    }
                }
            }

            await subscription.Completed(position).ConfigureAwait(false);
        }

        public async Task ReadPartitionBackward(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            var filter = Builders<Chunk>.Filter.And(
                Builders<Chunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<Chunk>.Filter.Lte(x => x.Index, fromUpperIndexInclusive),
                Builders<Chunk>.Filter.Gte(x => x.Index, toLowerIndexInclusive)
            );

            var sort = Builders<Chunk>.Sort.Descending(x => x.Index);
            var options = new FindOptions<Chunk>() { Sort = sort };
            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }

            await PushToSubscriber(fromUpperIndexInclusive, subscription, options, filter, cancellationToken).ConfigureAwait(false);
        }

        public async Task<IChunk> ReadLast(string partitionId, int toUpperIndexInclusive, CancellationToken cancellationToken)
        {
            var filter = Builders<Chunk>.Filter.And(
                Builders<Chunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<Chunk>.Filter.Lte(x => x.Index, toUpperIndexInclusive)
            );

            var sort = Builders<Chunk>.Sort.Descending(x => x.Index);
            var options = new FindOptions<Chunk>() { Sort = sort, Limit = 1 };

            using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
            {
                return await cursor.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task ReadAllAsync(long fromSequenceIdInclusive, ISubscription subscription, int limit, CancellationToken cancellationToken)
        {
            var filter = Builders<Chunk>.Filter.Gte(x => x.Position, fromSequenceIdInclusive);

            var options = new FindOptions<Chunk>()
            {
                Sort = Builders<Chunk>.Sort.Ascending(x => x.Position)
            };

            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }

            await PushToSubscriber(fromSequenceIdInclusive, subscription, options, filter, cancellationToken).ConfigureAwait(false);
        }

        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            var filter = Builders<Chunk>.Filter.Empty;
            var options = new FindOptions<Chunk>()
            {
                Sort = Builders<Chunk>.Sort.Descending(x => x.Position),
                Limit = 1
            };

            //TODO: project on position
            using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
            {
                var lastPosition = await cursor.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                return lastPosition?.Position ?? 0;
            }
        }

        public async Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId, CancellationToken cancellationToken)
        {
            long id = await GetNextId(cancellationToken).ConfigureAwait(false);
            var chunk = new Chunk()
            {
                Position = id,
                PartitionId = partitionId,
                Index = index < 0 ? id : index,
                Payload = _serializer.Serialize(partitionId, payload),
                OperationId = operationId ?? Guid.NewGuid().ToString()
            };

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
            var filterById = Builders<Chunk>.Filter.Eq(x => x.PartitionId, partitionId);
            if (fromLowerIndexInclusive > 0)
            {
                filterById = Builders<Chunk>.Filter.And(
                    filterById,
                    Builders<Chunk>.Filter.Gte(x => x.Index, fromLowerIndexInclusive)
                );
            }

            if (toUpperIndexInclusive < long.MaxValue)
            {
                filterById = Builders<Chunk>.Filter.And(
                    filterById,
                    Builders<Chunk>.Filter.Lte(x => x.Index, toUpperIndexInclusive)
                );
            }

            var result = await _chunks.DeleteManyAsync(filterById, cancellationToken).ConfigureAwait(false);
            if (!result.IsAcknowledged || result.DeletedCount == 0)
                throw new StreamDeleteException(partitionId);
        }

        private async Task PersistAsEmptyAsync(
            Chunk chunk,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            Chunk empty;
            //@@REVIEW partial index on mongo?
            if (chunk.PartitionId == "::empty")
            {
                // reuse chunk
                empty = chunk;
                empty.Index = empty.Position;
            }
            else
            {
                empty = new Chunk()
                {
                    Position = chunk.Position,
                    PartitionId = "::empty",
                    Index = chunk.Position,
                    Payload = null,
                    OperationId = "_" + chunk.Position
                };
            }
            await InternalPersistAsync(empty, cancellationToken).ConfigureAwait(false);
        }

        private async Task InternalPersistAsync(
            Chunk chunk,
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
                            chunk.Position = await GetNextId(cancellationToken).ConfigureAwait(false);
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

            _chunks = _partitionsDb.GetCollection<Chunk>(_options.PartitionsCollectionName);
            _counters = _countersDb.GetCollection<Counter>(_options.SequenceCollectionName);

            await _chunks.Indexes.CreateOneAsync(
                    Builders<Chunk>.IndexKeys
                        .Ascending(x => x.PartitionId)
                        .Ascending(x => x.Index),
                    new CreateIndexOptions()
                    {
                        Unique = true,
                        Name = SequenceIdx
                    }, cancellationToken)
                .ConfigureAwait(false);

            await _chunks.Indexes.CreateOneAsync(
                    Builders<Chunk>.IndexKeys
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
            var filter = Builders<Chunk>.Filter.Empty;
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