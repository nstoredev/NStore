using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace NStore.Mongo
{
	internal class MongoCommit
	{
		public long Id { get; set; }
		public string StreamId { get; set; }
		public long Index { get; set; }
		public object Payload { get; set; }
		public string OpId { get; set; }
	}

	internal class MongoId
	{
		public string Id { get; set; }
		public long LastValue { get; set; }
	}

	public class MongoStore : IStore
	{
		private IMongoDatabase _db;
		private IMongoCollection<MongoCommit> _events;
		private IMongoCollection<MongoId> _id;

		public MongoStore(IMongoDatabase db)
		{
			_db = db;
		}

		public async Task ScanAsync(string streamId, long indexStart, ScanDirection direction, Func<long, object, ScanCallbackResult> callback)
		{
			var filter = Builders<MongoCommit>.Filter.And(
				Builders<MongoCommit>.Filter.Eq(x => x.StreamId, streamId),
				Builders<MongoCommit>.Filter.Gte(x => x.Index, indexStart)
			);

			var sort = Builders<MongoCommit>.Sort.Ascending(x => x.Index);
			var options = new FindOptions<MongoCommit>() { Sort = sort };

			using (var cursor = await _events.FindAsync(filter, options))
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
			var doc = new MongoCommit()
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
			var commit = new MongoCommit()
			{
				Id = id,
				StreamId = "_empty",
				Index = id,
				Payload = null,
				OpId = "_" + id
			};

			await InternalPersistAsync(commit);
		}

		private async Task InternalPersistAsync(MongoCommit commit)
		{
			try
			{
				await _events.InsertOneAsync(commit);
			}
			catch (MongoWriteException ex)
			{
				if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
				{
					if (ex.Message.Contains("stream_operation"))
					{
						await PersistEmptyAsync(commit.Id);
					}
					return;
				}

				throw;
			}
		}

		public async Task InitAsync()
		{
			_events = _db.GetCollection<MongoCommit>("events");
			_id = _db.GetCollection<MongoId>("ids");

			await _events.Indexes.CreateOneAsync(
				Builders<MongoCommit>.IndexKeys
				 	.Ascending(x => x.StreamId)
					.Ascending(x => x.Index),
				new CreateIndexOptions()
				{
					Unique = true,
					Name = "stream_sequence"
				}
			);

			await _events.Indexes.CreateOneAsync(
				Builders<MongoCommit>.IndexKeys
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

			var filter = Builders<MongoId>.Filter.Eq(x => x.Id, "id");
			var update = Builders<MongoId>.Update.Inc(x => x.LastValue, 1);
			var options = new FindOneAndUpdateOptions<MongoId>()
			{
				IsUpsert = true,
				ReturnDocument = ReturnDocument.After
			};

			var updateResult = await _id.FindOneAndUpdateAsync(
				filter, update, options
			);

			return updateResult.LastValue;
		}
	}
}
