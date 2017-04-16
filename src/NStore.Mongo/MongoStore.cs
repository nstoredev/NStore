using System;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace NStore.Mongo
{
	internal class MongoCommit
	{
		public long Id { get; set; }
		public string StreamId { get; set; }
		public long Index { get; set; }
		public object Payload { get; set; }
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

		public Task GetAsync(string streamId, long indexStart, Action<long, object> callback)
		{
			throw new NotImplementedException();
		}

		public async Task PersistAsync(string streamId, long index, object payload)
		{
			long id = await GetNextId();
			var doc = new MongoCommit()
			{
				Id = id,
				StreamId = streamId,
				Index = index,
				Payload = payload
			};

			await _events.InsertOneAsync(doc);
		}

		public async Task InitAsync()
		{
			_events = _db.GetCollection<MongoCommit>("events");
			_id = _db.GetCollection<MongoId>("ids");

			await _events.Indexes.CreateOneAsync(
				Builders<MongoCommit>.IndexKeys
				 	.Ascending(x => x.StreamId)
					.Ascending(x => x.Index), 
				new CreateIndexOptions(){
					Unique = true
				}
			);
		}

		private async Task<long> GetNextId(){

			var filter = Builders<MongoId>.Filter.Eq(x => x.Id, "id");
			var update = Builders<MongoId>.Update.Inc(x => x.LastValue, 1);
			var options = new FindOneAndUpdateOptions<MongoId>()
			{
				IsUpsert = true,
				ReturnDocument = ReturnDocument.After
			};

			var updateResult = await _id.FindOneAndUpdateAsync(
				filter,update, options
			);

			return updateResult.LastValue;
		}
	}
}
