using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;

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
		public string StreamCollectionName { get; set; } = "streams";
		public string SequenceCollectionName { get; set; } = "seq";
		public string SequenceId { get; set; } = "streams";
		public bool UseLocalSequence { get; set; } = false;
	}

	public class MongoStore : IStore
	{
		private readonly IMongoDatabase _db;

		private IMongoCollection<Chunk> _chunks;
		private IMongoCollection<Counter> _counters;

		private readonly MongoStoreOptions _options;

		private long _sequence = 0;

		public MongoStore(IMongoDatabase db, MongoStoreOptions options = null)
		{
			_options = options ?? new MongoStoreOptions();
			_db = db;
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
				Index = index,
				Payload = payload,
				OpId = operationId ?? Guid.NewGuid().ToString()
			};

			await InternalPersistAsync(doc);
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
					if (ex.Message.Contains("stream_sequence"))
					{
						throw new DuplicateStreamIndexException(chunk.StreamId, chunk.Index);
					}

					if (ex.Message.Contains("stream_operation"))
					{
						await PersistEmptyAsync(chunk.Id);
						return;
					}
				}

				throw;
			}
		}

		public async Task InitAsync()
		{
			_chunks = _db.GetCollection<Chunk>(_options.StreamCollectionName);
			_counters = _db.GetCollection<Counter>(_options.SequenceCollectionName);

			await _chunks.Indexes.CreateOneAsync(
				Builders<Chunk>.IndexKeys
				 	.Ascending(x => x.StreamId)
					.Ascending(x => x.Index),
				new CreateIndexOptions()
				{
					Unique = true,
					Name = "stream_sequence"
				}
			);

			await _chunks.Indexes.CreateOneAsync(
				Builders<Chunk>.IndexKeys
					.Ascending(x => x.StreamId)
					.Ascending(x => x.OpId),
				new CreateIndexOptions()
				{
					Unique = true,
					Name = "stream_operation"
				}
			);
		}

	    private async Task<long> GetNextId()
	    {
			if(_options.UseLocalSequence)
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
	}
}
