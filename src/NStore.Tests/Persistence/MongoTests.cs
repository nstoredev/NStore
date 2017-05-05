using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NStore.Mongo;
using NStore.Raw;
using NStore.Raw.Contracts;
using Xunit;

namespace NStore.Tests.Persistence
{
    public class MongoFixture : IDisposable
    {
        public const string MONGO = "mongodb://localhost/nstore";

        public IRawStore Store { get; }

        public MongoFixture()
        {
            var options = new MongoStoreOptions
            {
                PartitionsConnectionString = MONGO,
                UseLocalSequence = true
            };
            Store = new MongoRawStore(options);
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

        protected IRawStore Store => _fixture.Store;

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

            Store.PersistAsync("Stream_2", 1, "d").Wait();
            Store.PersistAsync("Stream_2", 2, "e").Wait();
        }

        [Fact]
        public async Task ReadFirst()
        {
            object payload = null;

            await Store.ScanAsync(
                "Stream_1", 0, ScanDirection.Forward,
                (idx, pl) =>
                {
                    payload = pl;
                    return ScanCallbackResult.Stop;
                }
            );

            Assert.Equal("a", payload);
        }

        [Fact]
        public async Task ReadLast()
        {
            object payload = null;

            await Store.ScanAsync(
                "Stream_1", long.MaxValue, ScanDirection.Backward,
                (idx, pl) =>
                {
                    payload = pl;
                    return ScanCallbackResult.Stop;
                }
            );

            Assert.Equal("c", payload);
        }

        [Fact]
        public async Task should_read_only_first_two_chunks()
        {
            var buffer = new Tape();

            await Store.ScanAsync(
                "Stream_1", 0, ScanDirection.Forward,
                buffer.Record,
                2
            );

            Assert.Equal(2, buffer.Length);
            Assert.Equal("a", buffer[0]);
            Assert.Equal("b", buffer[1]);
        }

        [Fact]
        public async Task should_read_only_last_two_chunks()
        {
            var buffer = new Tape();

            await Store.ScanAsync(
                "Stream_1", long.MaxValue, ScanDirection.Backward,
                buffer.Record,
                2
            );

            Assert.Equal(2, buffer.Length);
            Assert.Equal("c", buffer[0]);
            Assert.Equal("b", buffer[1]);
        }

        [Fact]
        public async Task read_all_forward()
        {
            var buffer = new Tape();
            await Store.ScanStoreAsync(0, ScanDirection.Forward, buffer.Record);

            Assert.Equal(5, buffer.Length);
            Assert.Equal("a", buffer[0]);
            Assert.Equal("b", buffer[1]);
            Assert.Equal("c", buffer[2]);
            Assert.Equal("d", buffer[3]);
            Assert.Equal("e", buffer[4]);
        }

        [Fact]
        public async Task read_all_forward_from_middle()
        {
            var buffer = new Tape();
            await Store.ScanStoreAsync(3, ScanDirection.Forward, buffer.Record);

            Assert.Equal(3, buffer.Length);
            Assert.Equal("c", buffer[0]);
            Assert.Equal("d", buffer[1]);
            Assert.Equal("e", buffer[2]);
        }

        [Fact]
        public async Task read_all_forward_from_middle_limit_one()
        {
            var buffer = new Tape();
            await Store.ScanStoreAsync(3, ScanDirection.Forward, buffer.Record,1);

            Assert.Equal(1, buffer.Length);
            Assert.Equal("c", buffer[0]);
        }

        [Fact]
        public async Task read_all_backward()
        {
            var buffer = new Tape();
            await Store.ScanStoreAsync(long.MaxValue, ScanDirection.Backward, buffer.Record);

            Assert.Equal(5, buffer.Length);
            Assert.Equal("e", buffer[0]);
            Assert.Equal("d", buffer[1]);
            Assert.Equal("c", buffer[2]);
            Assert.Equal("b", buffer[3]);
            Assert.Equal("a", buffer[4]);
        }

        [Fact]
        public async Task read_all_backward_from_middle()
        {
            var buffer = new Tape();
            await Store.ScanStoreAsync(3, ScanDirection.Backward, buffer.Record);

            Assert.Equal(3, buffer.Length);
            Assert.Equal("c", buffer[0]);
            Assert.Equal("b", buffer[1]);
            Assert.Equal("a", buffer[2]);
        }

        [Fact]
        public async Task read_all_backward_from_middle_limit_one()
        {
            var buffer = new Tape();
            await Store.ScanStoreAsync(3, ScanDirection.Backward, buffer.Record,1);

            Assert.Equal(1, buffer.Length);
            Assert.Equal("c", buffer[0]);
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
                payload = (byte[]) p;
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
            await Store.PersistAsync("Stream_1", 1, new {data = "this is a test"});
        }

        [Fact]
        public async Task negative_index_should_persist_with_chunk_id()
        {
            Clear();
            await Store.PersistAsync("Stream_Neg", -1, "payload");

            var acc = new Tape();
            await Store.ScanAsync("Stream_Neg", 0, ScanDirection.Forward, acc.Record);
            Assert.Equal("payload", acc.ByIndex(1));
        }

        [Fact]
        public async Task can_insert_at_last_index()
        {
            Clear();
            await Store.PersistAsync("Stream_1", long.MaxValue, new {data = "this is a test"});
        }

        [Fact]
        public async Task insert_duplicate_chunk_index_should_throw()
        {
            Clear();
            await Store.PersistAsync("dup", 1, new {data = "first attempt"});
            await Store.PersistAsync("dup", 2, new {data = "should not work"});

            var ex = await Assert.ThrowsAnyAsync<DuplicateStreamIndexException>(() =>
                Store.PersistAsync("dup", 1, new {data = "this is a test"})
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
                Store.PersistAsync("Stream_2", i, new {data = "this is a test"})
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
            await Store.PersistAsync("Id_1", 0, new {data = "this is a test"}, opId);
            await Store.PersistAsync("Id_1", 1, new {data = "this is a test"}, opId);

            var list = new List<object>();
            await Store.ScanAsync("Id_1", 0, ScanDirection.Forward, (i, p) =>
            {
                list.Add(p);
                return ScanCallbackResult.Continue;
            });

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
            await Store.ScanAsync("Id_1", 0, ScanDirection.Forward, (i, p) =>
            {
                list.Add(p);
                return ScanCallbackResult.Continue;
            });
            await Store.ScanAsync("Id_2", 0, ScanDirection.Forward, (i, p) =>
            {
                list.Add(p);
                return ScanCallbackResult.Continue;
            });

            Assert.Equal(2, list.Count());
        }
    }

    [Collection("Mongo collection")]
    public class LocalSequenceTest
    {
        readonly IRawStore _store1;
        readonly IRawStore _store2;

        public LocalSequenceTest()
        {
            var options = new MongoStoreOptions()
            {
                PartitionsConnectionString = "mongodb://localhost/localseq",
                UseLocalSequence = true
            };

            _store1 = new MongoRawStore(options);
            _store2 = new MongoRawStore(options);

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

            var accumulator = new Tape();

            await _store1.ScanAsync("one", 0, ScanDirection.Forward, accumulator.Record);
            Assert.Equal(2, accumulator.Length);
        }
    }

    [Collection("Mongo collection")]
    public class DeleteStreamTest : AbstractMongoTest
    {
        public DeleteStreamTest(MongoFixture fixture) : base(fixture)
        {
            Clear();
            Task.WaitAll(
                Store.PersistAsync("delete", 1, null),
                Store.PersistAsync("delete_3", 1, "1"),
                Store.PersistAsync("delete_3", 2, "2"),
                Store.PersistAsync("delete_3", 3, "3"),
                Store.PersistAsync("delete_4", 1, "1"),
                Store.PersistAsync("delete_4", 2, "2"),
                Store.PersistAsync("delete_4", 3, "3"),
                Store.PersistAsync("delete_5", 1, "1"),
                Store.PersistAsync("delete_5", 2, "2"),
                Store.PersistAsync("delete_5", 3, "3")
            );
        }


        [Fact]
        public async void delete_stream()
        {
            await Store.DeleteAsync("delete");
            bool almostOneChunk = false;
            await Store.ScanAsync("delete", 0, ScanDirection.Forward, (l, o) =>
            {
                almostOneChunk = true;
                return ScanCallbackResult.Stop;
            });

            Assert.False(almostOneChunk, "Should not contains chunks");
        }

        [Fact]
        public async void delete_invalid_stream_should_throw_exception()
        {
            var ex = await Assert.ThrowsAnyAsync<StreamDeleteException>(() =>
                Store.DeleteAsync("delete_2")
            );

            Assert.Equal("delete_2", ex.StreamId);
        }

        [Fact]
        public async void should_delete_first()
        {
            await Store.DeleteAsync("delete_3", 1, 1);
            var acc = new Tape();
            await Store.ScanAsync("delete_3", 0, ScanDirection.Forward, acc.Record);

            Assert.Equal(2, acc.Length);
            Assert.True((string) acc[0] == "2");
            Assert.True((string) acc[1] == "3");
        }

        [Fact]
        public async void should_delete_last()
        {
            await Store.DeleteAsync("delete_4", 3);
            var acc = new Tape();
            await Store.ScanAsync("delete_4", 0, ScanDirection.Forward, acc.Record);

            Assert.Equal(2, acc.Length);
            Assert.True((string) acc[0] == "1");
            Assert.True((string) acc[1] == "2");
        }

        [Fact]
        public async void should_delete_middle()
        {
            await Store.DeleteAsync("delete_5", 2,2);
            var acc = new Tape();
            await Store.ScanAsync("delete_5", 0, ScanDirection.Forward, acc.Record);

            Assert.Equal(2, acc.Length);
            Assert.True((string) acc[0] == "1");
            Assert.True((string) acc[1] == "3");
        }
    }
}