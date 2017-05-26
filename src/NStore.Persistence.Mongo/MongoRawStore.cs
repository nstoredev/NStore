using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using NStore.Raw;

namespace NStore.Persistence.Mongo
{
    public class MongoRawStore : IRawStore
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

        public MongoRawStore(MongoStoreOptions options)
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

        public async Task Drop(CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            await this._partitionsDb
                .DropCollectionAsync(_options.PartitionsCollectionName, cancellationToken)
                .ConfigureAwait(false);

            await this._countersDb
                .DropCollectionAsync(_options.SequenceCollectionName, cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task ScanPartitionAsync(
            string partitionId,
            long fromIndexInclusive,
            ScanDirection direction,
            IPartitionConsumer partitionConsumer,
            long toIndexInclusive = Int64.MaxValue,
            int limit = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            var filter = Builders<Chunk>.Filter.And(
                Builders<Chunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<Chunk>.Filter.Gte(x => x.Index, fromIndexInclusive),
                Builders<Chunk>.Filter.Lte(x => x.Index, toIndexInclusive)
            );

            var sort = direction == ScanDirection.Forward
                ? Builders<Chunk>.Sort.Ascending(x => x.Index)
                : Builders<Chunk>.Sort.Descending(x => x.Index);

            var options = new FindOptions<Chunk>() { Sort = sort };
            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }

            using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
            {
                while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    var batch = cursor.Current;
                    foreach (var b in batch)
                    {
                        if (ScanAction.Stop ==
                            partitionConsumer.Consume(b.Index, _serializer.Deserialize(partitionId, b.Payload)))
                        {
                            return;
                        }
                    }
                }
            }
        }

        public async Task ScanStoreAsync(
            long sequenceStart,
            ScanDirection direction,
            IStoreConsumer consumer,
            int limit = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            SortDefinition<Chunk> sort;
            FilterDefinition<Chunk> filter;

            if (direction == ScanDirection.Forward)
            {
                sort = Builders<Chunk>.Sort.Ascending(x => x.Id);
                filter = Builders<Chunk>.Filter.Gte(x => x.Id, sequenceStart);
            }
            else
            {
                sort = Builders<Chunk>.Sort.Descending(x => x.Id);
                filter = Builders<Chunk>.Filter.Lte(x => x.Id, sequenceStart);
            }

            var options = new FindOptions<Chunk>() { Sort = sort };

            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }

            using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
            {
                while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    var batch = cursor.Current;
                    foreach (var chunk in batch)
                    {
                        if (ScanAction.Stop ==
                            await consumer.Consume(chunk.Id, chunk.PartitionId, chunk.Index, _serializer.Deserialize(chunk.PartitionId, chunk.Payload)))
                        {
                            return;
                        }
                    }
                }
            }
        }

        public async Task PersistAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            long id = await GetNextId(cancellationToken).ConfigureAwait(false);
            var doc = new Chunk()
            {
                Id = id,
                PartitionId = partitionId,
                Index = index < 0 ? id : index,
                Payload = _serializer.Serialize(partitionId, payload),
                OpId = operationId ?? Guid.NewGuid().ToString()
            };

            await InternalPersistAsync(doc, cancellationToken).ConfigureAwait(false);
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromIndex = 0,
            long toIndex = long.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            var filterById = Builders<Chunk>.Filter.Eq(x => x.PartitionId, partitionId);
            if (fromIndex > 0)
            {
                filterById = Builders<Chunk>.Filter.And(
                    filterById,
                    Builders<Chunk>.Filter.Gte(x => x.Index, fromIndex)
                );
            }

            if (toIndex < long.MaxValue)
            {
                filterById = Builders<Chunk>.Filter.And(
                    filterById,
                    Builders<Chunk>.Filter.Lte(x => x.Index, toIndex)
                );
            }

            var result = await _chunks.DeleteManyAsync(filterById, cancellationToken).ConfigureAwait(false);
            if (!result.IsAcknowledged || result.DeletedCount == 0)
                throw new StreamDeleteException(partitionId);
        }

        private async Task PersistAsEmptyAsync(Chunk chunk, CancellationToken cancellationToken = default(CancellationToken))
        {
            Chunk empty;
            //@@REVIEW partial index on mongo?
            if (chunk.PartitionId == "::empty")
            {
                // reuse chunk
                empty = chunk;
                empty.Index = empty.Id;
            }
            else
            {
                empty = new Chunk()
                {
                    Id = chunk.Id,
                    PartitionId = "::empty",
                    Index = chunk.Id,
                    Payload = null,
                    OpId = "_" + chunk.Id
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
                    //Console.WriteLine($"Error {ex.Message} - {ex.GetType().FullName}");

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
                                $"Error writing chunk #{chunk.Id} => {ex.Message} - {ex.GetType().FullName} ");
                            await ReloadSequence(cancellationToken).ConfigureAwait(false);
                            chunk.Id = await GetNextId(cancellationToken).ConfigureAwait(false);
                            continue;
                        }
                    }

                    throw;
                }
            }
        }

        public async Task InitAsync(CancellationToken cancellationToken = default(CancellationToken)
        )
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
                        .Ascending(x => x.OpId),
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
                .SortByDescending(x => x.Id)
                .Project(x => x.Id)
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

        //@@TODO remove
        public async Task DestroyStoreAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (this._partitionsDb != null)
            {
                await this._partitionsDb.Client
                    .DropDatabaseAsync(this._partitionsDb.DatabaseNamespace.DatabaseName, cancellationToken)
                    .ConfigureAwait(false);
            }

            _sequence = 0;
            _partitionsDb = null;
            _counters = null;
            _chunks = null;
        }
    }
}