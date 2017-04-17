using System;
using Xunit;
using MongoDB.Driver;
using NStore.Mongo;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace NStore.Tests
{
	public static class AsyncExtensions
	{
		public static Task ForEachAsync<T>(
		  this IEnumerable<T> source, int dop, Func<T, Task> body)
		{
			return Task.WhenAll(
				from partition in Partitioner.Create(source).GetPartitions(dop)
				select Task.Run(async delegate
				{
					using (partition)
						while (partition.MoveNext())
							await body(partition.Current).ContinueWith(t =>
								  {
									  //observe exceptions
								  });

				}));
		}		
	}

	public class MongoFixture : IDisposable
	{
		public IStore Store { get; }
		private IMongoDatabase _db;
		private MongoClient _client;
		private MongoUrl _url;

		public MongoFixture()
		{
			_url = new MongoUrl("mongodb://localhost/nstore");
			this._client = new MongoClient(new MongoClientSettings()
			{
				Server = _url.Server,
//				MaxConnectionPoolSize = 10000,
//				WaitQueueSize = 40000
			});

			this._db = _client.GetDatabase(_url.DatabaseName);
			Store = new MongoStore(this._db);
			Clear();
			Console.WriteLine("Start");
		}

		public void Dispose()
		{
			Console.WriteLine("Stop");
		}

		public void Clear()
		{
			_client.DropDatabase(_url.DatabaseName);
			Store.InitAsync().Wait();
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

	//	[Fact]
		public async Task InsertOne()
		{
			await Store.PersistAsync("Stream_1", 1, new { data = "this is a test" });
		}

		[Fact]
		public async void InsertMany()
		{
			await Worker(1, 10000);
			await Worker(2, 10000);
			await Worker(3, 10000);
			await Worker(4, 10000);
			await Worker(5, 10000);
			await Worker(6, 10000);
			await Worker(7, 10000);
			await Worker(8, 10000);
			await Worker(9, 10000);
			await Worker(10, 10000);
			await Worker(20, 10000);
		}

		private async Task Worker(int dop, int number)
		{
			this._fixture.Clear();

			int max = number;
			var range = Enumerable.Range(0, max);
			var sw = new Stopwatch();

			sw.Start();

			await range.ForEachAsync(dop, i =>
			   Store.PersistAsync("Stream_2", i, new { data = "this is a test" })
			);

			sw.Stop();

			Console.WriteLine($"Written {max} commits in {sw.ElapsedMilliseconds}ms using {dop} workers");
		}
	}
}
