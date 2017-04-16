using System;
using Xunit;
using MongoDB.Driver;
using NStore.Mongo;
using System.Threading.Tasks;

namespace NStore.Tests
{
	public class MongoFixture : IDisposable
	{
		public IStore Store { get; }
		private IMongoDatabase _db;
		public MongoFixture()
		{
			var uri = new MongoUrl("mongodb://localhost/nstore");
			var client = new MongoClient(uri);
			client.DropDatabase(uri.DatabaseName);
			this._db = client.GetDatabase(uri.DatabaseName);

			Store = new MongoStore(this._db);
			Console.WriteLine("Start");
			Store.InitAsync().Wait();
		}

		public void Dispose()
		{
			Console.WriteLine("Stop");

		}
	}

	[CollectionDefinition("Mongo collection")]
	public class MongoCollection : ICollectionFixture<MongoFixture>
	{
		
	}

	[Collection("Mongo collection")]
	public class MongoTests 
    {
		private MongoFixture _fixture;

		private IStore Store => _fixture.Store;

		public MongoTests(MongoFixture fixture)
		{
			this._fixture = fixture;
		}

		[Fact]
        public async Task InsertOne()
        {
			await Store.PersistAsync("Stream_1",1, new { data = "this is a test"});
        }
    }
}
