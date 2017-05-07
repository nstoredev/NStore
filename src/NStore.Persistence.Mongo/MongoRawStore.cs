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

        private readonly MongoStoreOptions _options;

        private long _sequence = 0;

        private const string SequenceIdx = "partition_sequence";
        private const string OperationIdx = "partition_operation";

        public MongoRawStore(MongoStoreOptions options)
        {
            if (options == null || !options.IsValid())
                throw new Exception("Invalid options");

            _options = options;

            Connect();
        }

        private void Connect()
        {
            var partitionsUrl = new MongoUrl(_options.PartitionsConnectionString);
            var partitionsClient = new MongoClient(partitionsUrl);
            this._partitionsDb = partitionsClient.GetDatabase(partitionsUrl.DatabaseName);

            if (_options.SequenceConnectionString == null)
            {
                this._countersDb = _partitionsDb;
            }
            else
            {
                var countersUrl = new MongoUrl(_options.SequenceConnectionString);
                var countersClient = new MongoClient(countersUrl);
                this._countersDb = countersClient.GetDatabase(countersUrl.DatabaseName);
            }
        }

        public async Task Drop()
        {
            await this._partitionsDb
                .DropCollectionAsync(_options.PartitionsCollectionName)
                .ConfigureAwait(false);

            await this._countersDb
                .DropCollectionAsync(_options.SequenceCollectionName)
                .ConfigureAwait(false);
        }

        public async Task ScanAsync(
            string partitionId,
            long indexStart,
            ScanDirection direction,
            Func<long, object, ScanCallbackResult> consume,
            int limit = int.MaxValue)
        {
            SortDefinition<Chunk> sort;
            FilterDefinition<Chunk> filter;

            if (direction == ScanDirection.Forward)
            {
                sort = Builders<Chunk>.Sort.Ascending(x => x.Index);
                filter = Builders<Chunk>.Filter.And(
                    Builders<Chunk>.Filter.Eq(x => x.PartitionId, partitionId),
                    Builders<Chunk>.Filter.Gte(x => x.Index, indexStart)
                );
            }
            else
            {
                sort = Builders<Chunk>.Sort.Descending(x => x.Index);
                filter = Builders<Chunk>.Filter.And(
                    Builders<Chunk>.Filter.Eq(x => x.PartitionId, partitionId),
                    Builders<Chunk>.Filter.Lte(x => x.Index, indexStart)
                );
            }

            var options = new FindOptions<Chunk>() {Sort = sort};

            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }

            using (var cursor = await _chunks.FindAsync(filter, options))
            {
                while (await cursor.MoveNextAsync())
                {
                    var batch = cursor.Current;
                    foreach (var b in batch)
                    {
                        if (ScanCallbackResult.Stop == consume(b.Index, b.Payload))
                        {
                            return;
                        }
                    }
                }
            }
        }

        public async Task ScanStoreAsync(long sequenceStart, ScanDirection direction,
            Func<long, object, ScanCallbackResult> consume, int limit = Int32.MaxValue)
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

            var options = new FindOptions<Chunk>() {Sort = sort};

            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }

            using (var cursor = await _chunks.FindAsync(filter, options))
            {
                while (await cursor.MoveNextAsync())
                {
                    var batch = cursor.Current;
                    foreach (var b in batch)
                    {
                        if (ScanCallbackResult.Stop == consume(b.Index, b.Payload))
                        {
                            return;
                        }
                    }
                }
            }
        }

        public async Task PersistAsync(string partitionId, long index, object payload, string operationId)
        {
            long id = await GetNextId();
            var doc = new Chunk()
            {
                Id = id,
                PartitionId = partitionId,
                Index = index < 0 ? id : index,
                Payload = payload,
                OpId = operationId ?? Guid.NewGuid().ToString()
            };

            await InternalPersistAsync(doc);
        }

        public async Task DeleteAsync(string partitionId, long fromIndex = 0, long toIndex = long.MaxValue)
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

            var result = await _chunks.DeleteManyAsync(filterById).ConfigureAwait(false);
            if (!result.IsAcknowledged || result.DeletedCount == 0)
                throw new StreamDeleteException(partitionId);
        }


        private async Task PersistEmptyAsync(long id)
        {
            var empty = new Chunk()
            {
                Id = id,
                PartitionId = "_empty",
                Index = id,
                Payload = null,
                OpId = "_" + id
            };

            await InternalPersistAsync(empty).ConfigureAwait(false);
        }

        private async Task InternalPersistAsync(Chunk chunk)
        {
            try
            {
                await _chunks.InsertOneAsync(chunk).ConfigureAwait(false);
            }
            catch (MongoWriteException ex)
            {
                //Console.WriteLine($"Error {ex.Message} - {ex.GetType().FullName}");

                if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                {
                    if (ex.Message.Contains(SequenceIdx))
                    {
                        throw new DuplicateStreamIndexException(chunk.PartitionId, chunk.Index);
                    }

                    if (ex.Message.Contains(OperationIdx))
                    {
                        //@@TODO skip instead of empty?
                        await PersistEmptyAsync(chunk.Id).ConfigureAwait(false);
                        return;
                    }

                    if (ex.Message.Contains("_id_"))
                    {
                        Console.WriteLine(
                            $"Error writing chunk #{chunk.Id} => {ex.Message} - {ex.GetType().FullName} ");
                        await ReloadSequence();
                        chunk.Id = await GetNextId();
                        //@@TODO move to while loop to avoid stack call
                        await InternalPersistAsync(chunk);
                        return;
                    }
                }

                throw;
            }
        }

        public async Task InitAsync()
        {
            if (_partitionsDb == null)
                Connect();

            if (_options.DropOnInit)
                Drop().Wait();

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
                }
            ).ConfigureAwait(false);

            await _chunks.Indexes.CreateOneAsync(
                Builders<Chunk>.IndexKeys
                    .Ascending(x => x.PartitionId)
                    .Ascending(x => x.OpId),
                new CreateIndexOptions()
                {
                    Unique = true,
                    Name = OperationIdx
                }
            ).ConfigureAwait(false);

            if (_options.UseLocalSequence)
            {
                await ReloadSequence().ConfigureAwait(false);
            }
        }

        private async Task ReloadSequence()
        {
            var filter = Builders<Chunk>.Filter.Empty;
            var lastSequenceNumber = await _chunks
                .Find(filter)
                .SortByDescending(x => x.Id)
                .Project(x => x.Id)
                .Limit(1)
                .FirstOrDefaultAsync()
                .ConfigureAwait(false);

            this._sequence = lastSequenceNumber;
        }

        private async Task<long> GetNextId()
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
                filter, update, options
            ).ConfigureAwait(false);

            return updateResult.LastValue;
        }

        //@@TODO remove
        public async Task DestroyStoreAsync()
        {
            if (this._partitionsDb != null)
            {
                await this._partitionsDb.Client.DropDatabaseAsync(this._partitionsDb.DatabaseNamespace.DatabaseName).ConfigureAwait(false);
            }

            _sequence = 0;
            _partitionsDb = null;
            _counters = null;
            _chunks = null;
        }
    }
}