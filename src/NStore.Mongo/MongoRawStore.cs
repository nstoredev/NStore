using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;
using NStore.Raw.Contracts;

namespace NStore.Mongo
{
    internal class Chunk
    {
        public long Id { get; set; }
        public string StreamId { get; set; }
        public long Index { get; set; }
        public object Payload { get; set; }
        public string OpId { get; set; }
    }

    internal class Counter
    {
        public string Id { get; set; }
        public long LastValue { get; set; }
    }

    public class MongoStoreOptions
    {
        public string StreamConnectionString { get; set; }
        public string StreamCollectionName { get; set; } = "streams";

        public string SequenceConnectionString { get; set; }
        public string SequenceCollectionName { get; set; } = "seq";
        public string SequenceId { get; set; } = "streams";
        public bool UseLocalSequence { get; set; } = false;

        public bool IsValid()
        {
            return !String.IsNullOrWhiteSpace(StreamConnectionString);
        }
    }

    public class MongoRawStore : IRawStore
    {
        private IMongoDatabase _streamsDb;

        private IMongoCollection<Chunk> _chunks;
        private IMongoCollection<Counter> _counters;

        private readonly MongoStoreOptions _options;
        private readonly MongoUrl _streamsUrl;

        private long _sequence = 0;

        private const string StreamSequenceIdx = "stream_sequence";
        private const string StreamOperationIdx = "stream_operation";

        public MongoRawStore(MongoStoreOptions options)
        {
            if (options == null || !options.IsValid())
                throw new Exception("Invalid options");

            _options = options;

            this._streamsUrl = new MongoUrl(options.StreamConnectionString);
            Connect();
        }

        private void Connect()
        {
            var client = new MongoClient(_streamsUrl);
            this._streamsDb = client.GetDatabase(_streamsUrl.DatabaseName);
        }

        public async Task ScanAsync(
            string streamId,
            long indexStart,
            ScanDirection direction,
            Func<long, object, ScanCallbackResult> callback,
            int limit = int.MaxValue)
        {
            SortDefinition<Chunk> sort;
            FilterDefinition<Chunk> filter;

            if (direction == ScanDirection.Forward)
            {
                sort = Builders<Chunk>.Sort.Ascending(x => x.Index);
                filter = Builders<Chunk>.Filter.And(
                    Builders<Chunk>.Filter.Eq(x => x.StreamId, streamId),
                    Builders<Chunk>.Filter.Gte(x => x.Index, indexStart)
                );
            }
            else
            {
                sort = Builders<Chunk>.Sort.Descending(x => x.Index);
                filter = Builders<Chunk>.Filter.And(
                    Builders<Chunk>.Filter.Eq(x => x.StreamId, streamId),
                    Builders<Chunk>.Filter.Lte(x => x.Index, indexStart)
                );
            }

            var options = new FindOptions<Chunk>() { Sort = sort };

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
                        if (ScanCallbackResult.Stop == callback(b.Index, b.Payload))
                        {
                            return;
                        }
                    }
                }
            }
        }

        public async Task PersistAsync(string streamId, long index, object payload, string operationId)
        {
            long id = await GetNextId();
            var doc = new Chunk()
            {
                Id = id,
                StreamId = streamId,
                Index = index < 0 ? id : index,
                Payload = payload,
                OpId = operationId ?? Guid.NewGuid().ToString()
            };

            await InternalPersistAsync(doc);
        }

        public async Task DeleteAsync(string streamId, long fromIndex = 0, long toIndex = long.MaxValue)
        {
            var filterById = Builders<Chunk>.Filter.Eq(x => x.StreamId, streamId);
            if (fromIndex > 0)
            {
                filterById = Builders<Chunk>.Filter.And(
                    filterById,
                    Builders<Chunk>.Filter.Gte(x=>x.Index, fromIndex)
                );
            }

            if (toIndex < long.MaxValue)
            {
                filterById = Builders<Chunk>.Filter.And(
                    filterById,
                    Builders<Chunk>.Filter.Lte(x=>x.Index, toIndex)
                );
            }

            var result = await _chunks.DeleteManyAsync(filterById);
            if (!result.IsAcknowledged || result.DeletedCount == 0)
                throw new StreamDeleteException(streamId);
        }


        private async Task PersistEmptyAsync(long id)
        {
            var empty = new Chunk()
            {
                Id = id,
                StreamId = "_empty",
                Index = id,
                Payload = null,
                OpId = "_" + id
            };

            await InternalPersistAsync(empty);
        }

        private async Task InternalPersistAsync(Chunk chunk)
        {
            try
            {
                await _chunks.InsertOneAsync(chunk);
            }
            catch (MongoWriteException ex)
            {
                //Console.WriteLine($"Error {ex.Message} - {ex.GetType().FullName}");

                if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                {
                    if (ex.Message.Contains(StreamSequenceIdx))
                    {
                        throw new DuplicateStreamIndexException(chunk.StreamId, chunk.Index);
                    }

                    if (ex.Message.Contains(StreamOperationIdx))
                    {
                        await PersistEmptyAsync(chunk.Id);
                        return;
                    }

                    if (ex.Message.Contains("_id_"))
                    {
                        Console.WriteLine($"Error writing chunk #{chunk.Id} => {ex.Message} - {ex.GetType().FullName} ");
                        await ReloadSequence();
                        chunk.Id = await GetNextId();
                        await InternalPersistAsync(chunk);
                        return;
                    }
                }

                throw;
            }
        }

        public async Task InitAsync()
        {
            if (_streamsDb == null)
                Connect();

            _chunks = _streamsDb.GetCollection<Chunk>(_options.StreamCollectionName);
            _counters = _streamsDb.GetCollection<Counter>(_options.SequenceCollectionName);

            await _chunks.Indexes.CreateOneAsync(
                Builders<Chunk>.IndexKeys
                     .Ascending(x => x.StreamId)
                    .Ascending(x => x.Index),
                new CreateIndexOptions()
                {
                    Unique = true,
                    Name = StreamSequenceIdx
                }
            );

            await _chunks.Indexes.CreateOneAsync(
                Builders<Chunk>.IndexKeys
                    .Ascending(x => x.StreamId)
                    .Ascending(x => x.OpId),
                new CreateIndexOptions()
                {
                    Unique = true,
                    Name = StreamOperationIdx
                }
            );

            if (_options.UseLocalSequence)
            {
                await ReloadSequence();
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
                .FirstOrDefaultAsync();

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
            );

            return updateResult.LastValue;
        }

        public async Task DestroyStoreAsync()
        {
            if (this._streamsDb != null)
            {
                await this._streamsDb.Client.DropDatabaseAsync(this._streamsDb.DatabaseNamespace.DatabaseName);
            }
            _sequence = 0;
            _streamsDb = null;
            _counters = null;
            _chunks = null;
        }
    }
}
