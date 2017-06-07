using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;
using Xunit;

// ReSharper disable InconsistentNaming

namespace NStore.Persistence.Tests
{
    public abstract partial class BasePersistenceTest : IDisposable
    {
        public IPersistence Store { get; }

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
            await Store.PersistAsync("Stream_1", 1, new { data = "this is a test" });
        }
    }

    public class negative_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_persist_with_chunk_id()
        {
            await Store.PersistAsync("Stream_Neg", -1, "payload");

            var tape = new Recorder();
            await Store.ReadPartitionForward("Stream_Neg", 0, tape);
            Assert.Equal("payload", tape.ByIndex(1));
        }
    }

    public class insert_at_last_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_work()
        {
            await Store.PersistAsync("Stream_1", long.MaxValue, new { data = "this is a test" });
        }
    }

    public class insert_duplicate_chunk_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_throw()
        {
            await Store.PersistAsync("dup", 1, new { data = "first attempt" });
            await Store.PersistAsync("dup", 2, new { data = "should not work" });

            var ex = await Assert.ThrowsAnyAsync<DuplicateStreamIndexException>(() =>
                Store.PersistAsync("dup", 1, new { data = "this is a test" })
            );

            Assert.Equal("Duplicated index 1 on stream dup", ex.Message);
            Assert.Equal("dup", ex.StreamId);
            Assert.Equal(1, ex.Index);
        }
    }

    internal static class AsyncExtensions
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

            await Store.ReadPartitionForward(
                "Stream_1", 0, new LambdaSubscription(data =>
               {
                   payload = data.Payload;
                   return Task.FromResult(false);
               })
            );

            Assert.Equal("a", payload);
        }

        [Fact]
        public async Task should_read_last_of_partition()
        {
            object payload = null;

            await Store.ReadPartitionBackward(
                "Stream_1",
                long.MaxValue,
                new LambdaSubscription(data =>
                {
                    payload = data.Payload;
                    return Task.FromResult(false);
                })
            );

            Assert.Equal("c", payload);
        }

        [Fact]
        public async Task should_read_only_first_two_chunks()
        {
            var recorder = new Recorder();

            await Store.ReadPartitionForward(
                "Stream_1", 0, recorder, 2
            );

            Assert.Equal(2, recorder.Length);
            Assert.Equal("a", recorder[0]);
            Assert.Equal("b", recorder[1]);
        }

        [Fact]
        public async Task read_forward_should_call_complete_on_consumer()
        {
            var recorder = new Recorder();

            await Store.ReadPartitionForward(
                "Stream_1", 0, recorder, 2
            );

            Assert.True(recorder.ReadCompleted);
        }

        [Fact]
        public async Task read_backward_should_call_complete_on_consumer()
        {
            var recorder = new Recorder();

            await Store.ReadPartitionBackward(
                "Stream_1", 2, recorder, 0
            );

            Assert.True(recorder.ReadCompleted);
        }


        [Fact]
        public async Task should_read_only_last_two_chunks()
        {
            var tape = new Recorder();

            await Store.ReadPartitionBackward(
                "Stream_1",
                3,
                tape,
                2
            );

            Assert.Equal(2, tape.Length);
            Assert.Equal("c", tape[0]);
            Assert.Equal("b", tape[1]);
        }

        [Fact]
        public async Task read_all_forward()
        {
            var tape = new AllPartitionsRecorder();
            await Store.ReadAllAsync(0, tape);

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
            var tape = new AllPartitionsRecorder();
            await Store.ReadAllAsync(3, tape);

            Assert.Equal(3, tape.Length);
            Assert.Equal("c", tape[0]);
            Assert.Equal("d", tape[1]);
            Assert.Equal("e", tape[2]);
        }

        [Fact]
        public async Task read_all_forward_from_middle_limit_one()
        {
            var tape = new AllPartitionsRecorder();
            await Store.ReadAllAsync(3, tape, 1);

            Assert.Equal(1, tape.Length);
            Assert.Equal("c", tape[0]);
        }
    }

    public class ByteArrayPersistenceTest : BasePersistenceTest
    {
        [Fact]
        public async Task InsertByteArray()
        {
            await Store.PersistAsync("BA", 0, System.Text.Encoding.UTF8.GetBytes("this is a test"));

            byte[] payload = null;
            await Store.ReadPartitionForward("BA", 0, new LambdaSubscription(data =>
            {
                payload = (byte[])data.Payload;
                return Task.FromResult(true);
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
            await Store.PersistAsync("Id_1", 0, new { data = "this is a test" }, opId);
            await Store.PersistAsync("Id_1", 1, new { data = "this is a test" }, opId);

            var list = new List<object>();
            await Store.ReadPartitionForward("Id_1", 0, new LambdaSubscription(data =>
            {
                list.Add(data.Payload);
                return Task.FromResult(true);
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
            await Store.ReadPartitionForward("Id_1", 0, new LambdaSubscription(data =>
            {
                list.Add(data.Payload);
                return Task.FromResult(true);
            }));
            await Store.ReadPartitionForward("Id_2", 0, new LambdaSubscription(data =>
            {
                list.Add(data.Payload);
                return Task.FromResult(true);
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
            await Store.ReadPartitionForward("delete", 0, new LambdaSubscription(data =>
            {
                almostOneChunk = true;
                return Task.FromResult(false);
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
            var acc = new Recorder();
            await Store.ReadPartitionForward("delete_3", 0, acc);

            Assert.Equal(2, acc.Length);
            Assert.True((string)acc[0] == "2");
            Assert.True((string)acc[1] == "3");
        }

        [Fact]
        public async void should_delete_last()
        {
            await Store.DeleteAsync("delete_4", 3);
            var acc = new Recorder();
            await Store.ReadPartitionForward("delete_4", 0, acc);

            Assert.Equal(2, acc.Length);
            Assert.True((string)acc[0] == "1");
            Assert.True((string)acc[1] == "2");
        }

        [Fact]
        public async void should_delete_middle()
        {
            await Store.DeleteAsync("delete_5", 2, 2);
            var acc = new Recorder();
            await Store.ReadPartitionForward("delete_5", 0, acc);

            Assert.Equal(2, acc.Length);
            Assert.True((string)acc[0] == "1");
            Assert.True((string)acc[1] == "3");
        }
    }

    public class concurrency_test : BasePersistenceTest
    {
        [Fact]
        public async void polling_client_should_not_miss_data()
        {
            var recorder = new AllPartitionsRecorder();

            var poller = new PollingClient(Store, recorder)
            {
                Delay = 100
            };

            poller.Start();

            const int range = 2048;
            await Enumerable.Range(1, range).ForEachAsync(32,
                async i => { await Store.PersistAsync("p", -1, "demo"); }
            )
            .ConfigureAwait(false);

            await Task.Delay(1000);

            poller.Stop();

            //Console.WriteLine("Dumping recorder");
            //recorder.Replay((storeIndex, partitionId, index, payload) =>
            //{
            //    Console.WriteLine($"{storeIndex:D5} - {partitionId.PadRight(20)} - {index:D5}");
            //});

            Assert.Equal(range, poller.Position);
            Assert.Equal(range, recorder.Length);
        }
    }

    public class strict_sequence_on_store : BasePersistenceTest
    {
        [Fact]
        public async void on_concurrency_exception_holes_are_filled_with_empty_chunks()
        {
            var exceptions = 0;
            var writers = Enumerable.Range(1, 400).Select(async i =>
               {
                   try
                   {
                       await Store.PersistAsync("collision_wanted", 1 + i % 5, "payload");
                   }
                   catch (DuplicateStreamIndexException)
                   {
                       Interlocked.Increment(ref exceptions);
                   }
               }
            ).ToArray();

            Task.WaitAll(writers);

            Assert.True(exceptions > 0);
            var recorder = new Recorder();
            await Store.ReadPartitionForward("::empty", 0, recorder);

            Assert.Equal(exceptions, recorder.Length);
        }
    }
}