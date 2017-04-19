using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;
using NStore.Mongo;
using Xunit;

namespace NStore.Tests.Persistence
{
	public class MongoFixture : IDisposable
	{
		public const string MONGO = "mongodb://localhost/nstore";

		public IStore Store { get; }

		public MongoFixture()
		{
			var options = new MongoStoreOptions
			{
				StreamConnectionString = MONGO,
				UseLocalSequence = true
			};
			Store = new MongoStore(options);
			Clear().Wait();
		}

		public void Dispose()
		{
		}

		public async Task Clear()
		{
			try
			{
				await Store.DestroyStoreAsync();
				await Store.InitAsync();
			}
			catch (Exception ex)
			{
				Console.WriteLine($"ERROR: {ex.Message}");
			}
		}
	}

	[CollectionDefinition("Mongo collection")]
	public class MongoCollection : ICollectionFixture<MongoFixture>
	{

	}

	[Collection("Mongo collection")]
	public abstract class AbstractMongoTest
	{
		private readonly MongoFixture _fixture;

		protected IStore Store => _fixture.Store;

		protected AbstractMongoTest(MongoFixture fixture)
		{
			this._fixture = fixture;
		}

		protected void Clear()
		{
			this._fixture.Clear().Wait();
		}
	}

	public class ScanTest : AbstractMongoTest
	{
		public ScanTest(MongoFixture fixture) : base(fixture)
		{
			Clear();
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

	public class MongoByteArrayTests : AbstractMongoTest
	{
		public MongoByteArrayTests(MongoFixture fixture) : base(fixture)
		{
		}

		[Fact]
		public async Task InsertByteArray()
		{
			Clear();
			await Store.PersistAsync("BA", 0, System.Text.Encoding.UTF8.GetBytes("this is a test"));

			byte[] payload = null;
			await Store.ScanAsync("BA", 0, ScanDirection.Forward, (i, p) =>
			{
				payload = (byte[])p;
				return ScanCallbackResult.Continue;
			});

			var text = System.Text.Encoding.UTF8.GetString(payload);
			Assert.Equal("this is a test", text);
		}
	}


	public class MongoWriteTests : AbstractMongoTest
	{
		public MongoWriteTests(MongoFixture fixture) : base(fixture)
		{
		}

		[Fact]
		public async Task can_insert_at_first_index()
		{
			Clear();
			await Store.PersistAsync("Stream_1", 1, new { data = "this is a test" });
		}

		[Fact]
		public async Task can_insert_at_last_index()
		{
			Clear();
			await Store.PersistAsync("Stream_1", long.MaxValue, new { data = "this is a test" });
		}

		[Fact]
		public async Task insert_duplicate_chunk_index_should_throw()
		{
			Clear();
			await Store.PersistAsync("dup", 1, new { data = "first attempt" });
			await Store.PersistAsync("dup", 2, new { data = "should not work" });

			var ex = await Assert.ThrowsAnyAsync<DuplicateStreamIndexException>(() =>
				 Store.PersistAsync("dup", 1, new { data = "this is a test" })
			);

			Assert.Equal("Duplicated index 1 on stream dup", ex.Message);
			Assert.Equal("dup", ex.StreamId);
			Assert.Equal(1, ex.Index);
		}

		[Fact(Skip = "long running")]
		//[Fact]
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

	[Collection("Mongo collection")]
	public class LocalSequenceTest
	{
		IStore _store1;
		IStore _store2;

		public LocalSequenceTest()
		{
			var options = new MongoStoreOptions()
			{
				StreamConnectionString = "mongodb://localhost/localseq",
				UseLocalSequence = true
			};

			_store1 = new MongoStore(options);
			_store2 = new MongoStore(options);

			Task.WaitAll(
				_store1.DestroyStoreAsync()
			);

			Task.WaitAll(
				_store1.InitAsync(),
				_store2.InitAsync()
			);
		}

		[Fact]
		public async void collision_reload_sequence()
		{
			await _store1.PersistAsync("one", 1, null, "op1");
			await _store2.PersistAsync("one", 2, null, "op2");

			var accumulator = new Accumulator();

			await _store1.ScanAsync("one", 0, ScanDirection.Forward, accumulator.Consume);
			Assert.Equal(2, accumulator.Length);
		}
	}
}
