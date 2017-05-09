using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NStore.Raw;
using Xunit;

// ReSharper disable InconsistentNaming

namespace NStore.Persistence.Tests
{
    public abstract partial class BasePersistenceTest : IDisposable
    {
        public IRawStore Store { get; }

        protected BasePersistenceTest()
        {
            Store = Create();
        }

        public void Dispose()
        {
            Clear();
        }
    }

    public class WriteTests : BasePersistenceTest
    {
        [Fact]
        public async Task can_insert_at_first_index()
        {
            await Store.PersistAsync("Stream_1", 1, new {data = "this is a test"});
        }
    }

    public class negative_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_persist_with_chunk_id()
        {
            await Store.PersistAsync("Stream_Neg", -1, "payload");

            var tape = new Tape();
            await Store.ScanPartitionAsync("Stream_Neg", 0, ScanDirection.Forward, tape);
            Assert.Equal("payload", tape.ByIndex(1));
        }
    }

    public class insert_at_last_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_work()
        {
            await Store.PersistAsync("Stream_1", long.MaxValue, new {data = "this is a test"});
        }
    }

    public class insert_duplicate_chunk_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_throw()
        {
            await Store.PersistAsync("dup", 1, new {data = "first attempt"});
            await Store.PersistAsync("dup", 2, new {data = "should not work"});

            var ex = await Assert.ThrowsAnyAsync<DuplicateStreamIndexException>(() =>
                Store.PersistAsync("dup", 1, new {data = "this is a test"})
            );

            Assert.Equal("Duplicated index 1 on stream dup", ex.Message);
            Assert.Equal("dup", ex.StreamId);
            Assert.Equal(1, ex.Index);
        }
    }

    public class long_running_test : BasePersistenceTest
    {
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
                            await body(partition.Current)
                                .ContinueWith(t =>
                                {
                                    //observe exceptions
                                });
                }));
        }
    }

    public class ScanTest : BasePersistenceTest
    {
        public ScanTest()
        {
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

            await Store.ScanPartitionAsync(
                "Stream_1", 0, ScanDirection.Forward,
                new LambdaConsumer((idx, pl) =>
                {
                    payload = pl;
                    return ScanCallbackResult.Stop;
                })
            );

            Assert.Equal("a", payload);
        }

        [Fact]
        public async Task should_read_last_of_partition()
        {
            object payload = null;

            await Store.ScanPartitionAsync(
                "Stream_1",
                0,
                ScanDirection.Backward,
                new LambdaConsumer((idx, pl) =>
                {
                    payload = pl;
                    return ScanCallbackResult.Stop;
                })
            );

            Assert.Equal("c", payload);
        }

        [Fact]
        public async Task should_read_only_first_two_chunks()
        {
            var tape = new Tape();

            await Store.ScanPartitionAsync(
                "Stream_1", 0, ScanDirection.Forward,
                tape,
                2
            );

            Assert.Equal(2, tape.Length);
            Assert.Equal("a", tape[0]);
            Assert.Equal("b", tape[1]);
        }

        [Fact]
        public async Task should_read_only_last_two_chunks()
        {
            var tape = new Tape();

            await Store.ScanPartitionAsync(
                "Stream_1",
                2,
                ScanDirection.Backward,
                tape,
                3
            );

            Assert.Equal(2, tape.Length);
            Assert.Equal("c", tape[0]);
            Assert.Equal("b", tape[1]);
        }

        [Fact]
        public async Task read_all_forward()
        {
            var tape = new Tape();
            await Store.ScanStoreAsync(0, ScanDirection.Forward, tape);

            Assert.Equal(5, tape.Length);
            Assert.Equal("a", tape[0]);
            Assert.Equal("b", tape[1]);
            Assert.Equal("c", tape[2]);
            Assert.Equal("d", tape[3]);
            Assert.Equal("e", tape[4]);
        }

        [Fact]
        public async Task read_all_forward_from_middle()
        {
            var tape = new Tape();
            await Store.ScanStoreAsync(
                3,
                ScanDirection.Forward,
                tape
            );

            Assert.Equal(3, tape.Length);
            Assert.Equal("c", tape[0]);
            Assert.Equal("d", tape[1]);
            Assert.Equal("e", tape[2]);
        }

        [Fact]
        public async Task read_all_forward_from_middle_limit_one()
        {
            var tape = new Tape();
            await Store.ScanStoreAsync(
                3,
                ScanDirection.Forward,
                tape,
                1
               );

            Assert.Equal(1, tape.Length);
            Assert.Equal("c", tape[0]);
        }

        [Fact]
        public async Task read_all_backward()
        {
            var buffer = new Tape();
            await Store.ScanStoreAsync(
                long.MaxValue,
                ScanDirection.Backward,
                buffer
            );

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
            await Store.ScanStoreAsync(
                3,
                ScanDirection.Backward,
                buffer
            );

            Assert.Equal(3, buffer.Length);
            Assert.Equal("c", buffer[0]);
            Assert.Equal("b", buffer[1]);
            Assert.Equal("a", buffer[2]);
        }

        [Fact]
        public async Task read_all_backward_from_middle_limit_one()
        {
            var buffer = new Tape();
            await Store.ScanStoreAsync(3, ScanDirection.Backward, buffer, 1);

            Assert.Equal(1, buffer.Length);
            Assert.Equal("c", buffer[0]);
        }
    }

    public class ByteArrayPersistenceTest : BasePersistenceTest
    {
        [Fact]
        public async Task InsertByteArray()
        {
            await Store.PersistAsync("BA", 0, System.Text.Encoding.UTF8.GetBytes("this is a test"));

            byte[] payload = null;
            await Store.ScanPartitionAsync("BA", 0, ScanDirection.Forward, new LambdaConsumer((i, p) =>
            {
                payload = (byte[]) p;
                return ScanCallbackResult.Continue;
            }));

            var text = System.Text.Encoding.UTF8.GetString(payload);
            Assert.Equal("this is a test", text);
        }
    }

    public class IdempotencyTest : BasePersistenceTest
    {
        [Fact]
        public async Task cannot_append_same_operation_twice_on_same_stream()
        {
            var opId = "operation_1";
            await Store.PersistAsync("Id_1", 0, new {data = "this is a test"}, opId);
            await Store.PersistAsync("Id_1", 1, new {data = "this is a test"}, opId);

            var list = new List<object>();
            await Store.ScanPartitionAsync("Id_1", 0, ScanDirection.Forward, new LambdaConsumer((i, p) =>
            {
                list.Add(p);
                return ScanCallbackResult.Continue;
            }));

            Assert.Equal(1, list.Count());
        }

        [Fact]
        public async Task can_append_same_operation_to_two_streams()
        {
            var opId = "operation_2";
            await Store.PersistAsync("Id_1", 0, "a", opId);
            await Store.PersistAsync("Id_2", 1, "b", opId);

            var list = new List<object>();
            await Store.ScanPartitionAsync("Id_1", 0, ScanDirection.Forward, new LambdaConsumer((i, p) =>
            {
                list.Add(p);
                return ScanCallbackResult.Continue;
            }));
            await Store.ScanPartitionAsync("Id_2", 0, ScanDirection.Forward, new LambdaConsumer((i, p) =>
            {
                list.Add(p);
                return ScanCallbackResult.Continue;
            }));

            Assert.Equal(2, list.Count());
        }
    }

    public class DeleteStreamTest : BasePersistenceTest
    {
        public DeleteStreamTest()
        {
            Task.WaitAll
            (
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
            await Store.ScanPartitionAsync("delete", 0, ScanDirection.Forward, new LambdaConsumer((l, o) =>
            {
                almostOneChunk = true;
                return ScanCallbackResult.Stop;
            }));

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
            await Store.ScanPartitionAsync("delete_3", 0, ScanDirection.Forward, acc);

            Assert.Equal(2, acc.Length);
            Assert.True((string) acc[0] == "2");
            Assert.True((string) acc[1] == "3");
        }

        [Fact]
        public async void should_delete_last()
        {
            await Store.DeleteAsync("delete_4", 3);
            var acc = new Tape();
            await Store.ScanPartitionAsync("delete_4", 0, ScanDirection.Forward, acc);

            Assert.Equal(2, acc.Length);
            Assert.True((string) acc[0] == "1");
            Assert.True((string) acc[1] == "2");
        }

        [Fact]
        public async void should_delete_middle()
        {
            await Store.DeleteAsync("delete_5", 2, 2);
            var acc = new Tape();
            await Store.ScanPartitionAsync("delete_5", 0, ScanDirection.Forward, acc);

            Assert.Equal(2, acc.Length);
            Assert.True((string) acc[0] == "1");
            Assert.True((string) acc[1] == "3");
        }
    }
}