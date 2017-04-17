using System;
using Xunit;
using MongoDB.Driver;
using NStore.Mongo;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Linq;
using System.Collections.Generic;

namespace NStore.Tests
{
	public class MongoFixture : IDisposable
	{
		public IStore Store { get; }
		private IMongoDatabase _db;
		private MongoClient _client;
		private MongoUrl _url;

		public MongoFixture()
		{
			_url = new MongoUrl("mongodb://localhost/nstore");
			this._client = new MongoClient(_url);

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
	public abstract class AbstractMongoTest
	{
		private MongoFixture _fixture;

		protected IStore Store => _fixture.Store;

		public AbstractMongoTest(MongoFixture fixture)
		{
			this._fixture = fixture;
		}

		protected void Clear()
		{
			this._fixture.Clear();
		}
	}

	public class ScanTest : AbstractMongoTest
	{
		public ScanTest(MongoFixture fixture) : base(fixture)
		{
			Store.PersistAsync("Stream_1", 1, "a").Wait();
			Store.PersistAsync("Stream_1", 2, "b").Wait();
			Store.PersistAsync("Stream_1", 3, "c").Wait();
		}

		[Fact]
		public async Task ReadFirst()
		{
			object payload = null;

			await Store.ScanAsync(
				"Stream_1", 0, ScanDirection.Forward,
				(idx, pl) => { payload = pl; return ScanCallbackResult.Stop; }
			);

			Assert.Equal("a", payload);
		}

		[Fact]
		public async Task ReadLast()
		{
			object payload = null;

			await Store.ScanAsync(
				"Stream_1", long.MaxValue, ScanDirection.Backward,
				(idx, pl) => { payload = pl; return ScanCallbackResult.Stop; }
			);

			Assert.Equal("c", payload);
		}

		[Fact]
		public async Task should_read_only_first_two_chunks()
		{
			var buffer = new Accumulator();

			await Store.ScanAsync(
				"Stream_1", 0, ScanDirection.Forward,
				buffer.Consume,
				2
			);

			Assert.Equal(2, buffer.Length);
			Assert.Equal("a", buffer[0]);
			Assert.Equal("b", buffer[1]);
		}

		[Fact]
		public async Task should_read_only_last_two_chunks()
		{
			var buffer = new Accumulator();

			await Store.ScanAsync(
				"Stream_1", long.MaxValue, ScanDirection.Backward,
				buffer.Consume,
				2
			);

			Assert.Equal(2, buffer.Length);
			Assert.Equal("c", buffer[0]);
			Assert.Equal("b", buffer[1]);
		}
	}

	public class MongoWriteTests : AbstractMongoTest
	{
		public MongoWriteTests(MongoFixture fixture) : base(fixture)
		{
		}

		[Fact]
		public async Task InsertOne()
		{
			Clear();
			await Store.PersistAsync("Stream_1", 1, new { data = "this is a test" });
		}

		[Fact]
		public async Task InsertLast()
		{
			Clear();
			await Store.PersistAsync("Stream_1", long.MaxValue, new { data = "this is a test" });
		}



		[Fact(Skip = "long running")]
		public async Task InsertMany()
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
			this.Clear();

			int max = number;
			var range = Enumerable.Range(0, max);
			var sw = new Stopwatch();

			sw.Start();

			await range.ForEachAsync(dop, i =>
			   Store.PersistAsync("Stream_2", i, new { data = "this is a test" })
			);

			sw.Stop();

			Console.WriteLine($"Written {max} chunks in {sw.ElapsedMilliseconds}ms using {dop} workers");
		}
	}

	public class IdempotencyTest : AbstractMongoTest
	{
		public IdempotencyTest(MongoFixture fixture) : base(fixture)
		{
		}

		[Fact]
		public async Task cannot_append_same_operation_twice_on_same_stream()
		{
			Clear();
			var opId = "operation_1";
			await Store.PersistAsync("Id_1", 0, new { data = "this is a test" }, opId);
			await Store.PersistAsync("Id_1", 1, new { data = "this is a test" }, opId);

			var list = new List<object>();
			await Store.ScanAsync("Id_1", 0, ScanDirection.Forward, (i, p) => { list.Add(p); return ScanCallbackResult.Continue; });

			Assert.Equal(1, list.Count());
		}

		[Fact]
		public async Task can_append_same_operation_to_two_streams()
		{
			Clear();

			var opId = "operation_2";
			await Store.PersistAsync("Id_1", 0, "a", opId);
			await Store.PersistAsync("Id_2", 1, "b", opId);

			var list = new List<object>();
			await Store.ScanAsync("Id_1", 0, ScanDirection.Forward, (i, p) => { list.Add(p); return ScanCallbackResult.Continue; });
			await Store.ScanAsync("Id_2", 0, ScanDirection.Forward, (i, p) => { list.Add(p); return ScanCallbackResult.Continue; });

			Assert.Equal(2, list.Count());
		}
	}
}
