using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;
using Xunit;

// ReSharper disable InconsistentNaming
namespace NStore.Persistence.Tests
{
    public abstract partial class BasePersistenceTest : IDisposable
    {
        protected IPersistence Store { get; }
        protected readonly TestLoggerFactory LoggerFactory;
        protected readonly ILogger _logger;
        protected BasePersistenceTest()
        {
            LoggerFactory = new TestLoggerFactory(TestSuitePrefix +"::"+ GetType().Name);
            _logger = LoggerFactory.CreateLogger(GetType());
            Store = new LogDecorator(Create(), LoggerFactory);
        }

        public void Dispose()
        {
            Clear();
            _logger.LogDebug("Test disposed");
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
            Store.PersistAsync("Stream_1", 1, "a").ConfigureAwait(false).GetAwaiter().GetResult();
            Store.PersistAsync("Stream_1", 2, "b").ConfigureAwait(false).GetAwaiter().GetResult();
            Store.PersistAsync("Stream_1", 3, "c").ConfigureAwait(false).GetAwaiter().GetResult();

            Store.PersistAsync("Stream_2", 1, "d").ConfigureAwait(false).GetAwaiter().GetResult();
            Store.PersistAsync("Stream_2", 2, "e").ConfigureAwait(false).GetAwaiter().GetResult();
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
            ).ConfigureAwait(false);

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
            ).ConfigureAwait(false);

            Assert.Equal("c", payload);
        }

        [Fact]
        public async Task should_read_only_first_two_chunks()
        {
            var recorder = new Recorder();

            await Store.ReadPartitionForward(
                "Stream_1", 0, recorder, 2
            ).ConfigureAwait(false);

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
            ).ConfigureAwait(false);

            Assert.True(recorder.ReadCompleted);
        }

        [Fact]
        public async Task read_backward_should_call_complete_on_consumer()
        {
            var recorder = new Recorder();

            await Store.ReadPartitionBackward(
                "Stream_1", 2, recorder, 0
            ).ConfigureAwait(false);

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
            ).ConfigureAwait(false);

            Assert.Equal(2, tape.Length);
            Assert.Equal("c", tape[0]);
            Assert.Equal("b", tape[1]);
        }

        [Fact]
        public async Task read_all_forward()
        {
            var tape = new AllPartitionsRecorder();
            await Store.ReadAllAsync(0, tape).ConfigureAwait(false);

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
            await Store.ReadAllAsync(3, tape).ConfigureAwait(false);

            Assert.Equal(3, tape.Length);
            Assert.Equal("c", tape[0]);
            Assert.Equal("d", tape[1]);
            Assert.Equal("e", tape[2]);
        }

        [Fact]
        public async Task read_all_forward_from_middle_limit_one()
        {
            var tape = new AllPartitionsRecorder();
            await Store.ReadAllAsync(3, tape, 1).ConfigureAwait(false);

            Assert.Equal(1, tape.Length);
            Assert.Equal("c", tape[0]);
        }
    }

    public class ByteArrayPersistenceTest : BasePersistenceTest
    {
        [Fact]
        public async Task InsertByteArray()
        {
            await Store.PersistAsync("BA", 0, System.Text.Encoding.UTF8.GetBytes("this is a test")).ConfigureAwait(false);

            byte[] payload = null;
            await Store.ReadPartitionForward("BA", 0, new LambdaSubscription(data =>
            {
                payload = (byte[])data.Payload;
                return Task.FromResult(true);
            })).ConfigureAwait(false);

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
            await Store.PersistAsync("Id_1", 0, new { data = "this is a test" }, opId).ConfigureAwait(false);
            await Store.PersistAsync("Id_1", 1, new { data = "this is a test" }, opId).ConfigureAwait(false);

            var list = new List<object>();
            await Store.ReadPartitionForward("Id_1", 0, new LambdaSubscription(data =>
            {
                list.Add(data.Payload);
                return Task.FromResult(true);
            })).ConfigureAwait(false);

            Assert.Equal(1, list.Count());
        }

        [Fact]
        public async Task can_append_same_operation_to_two_streams()
        {
            var opId = "operation_2";
            await Store.PersistAsync("Id_1", 0, "a", opId).ConfigureAwait(false);
            await Store.PersistAsync("Id_2", 1, "b", opId).ConfigureAwait(false);

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
            Task.WhenAll
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
            ).ConfigureAwait(false).GetAwaiter().GetResult();
        }


        [Fact]
        public async void delete_stream()
        {
            await Store.DeleteAsync("delete").ConfigureAwait(false);
            bool almostOneChunk = false;
            await Store.ReadPartitionForward("delete", 0, new LambdaSubscription(data =>
            {
                almostOneChunk = true;
                return Task.FromResult(false);
            })).ConfigureAwait(false);

            Assert.False(almostOneChunk, "Should not contains chunks");
        }

        [Fact]
        public async void delete_invalid_stream_should_throw_exception()
        {
            var ex = await Assert.ThrowsAnyAsync<StreamDeleteException>(() =>
                Store.DeleteAsync("delete_2")
            ).ConfigureAwait(false);

            Assert.Equal("delete_2", ex.StreamId);
        }

        [Fact]
        public async void should_delete_first()
        {
            await Store.DeleteAsync("delete_3", 1, 1).ConfigureAwait(false);
            var acc = new Recorder();
            await Store.ReadPartitionForward("delete_3", 0, acc).ConfigureAwait(false);

            Assert.Equal(2, acc.Length);
            Assert.True((string)acc[0] == "2");
            Assert.True((string)acc[1] == "3");
        }

        [Fact]
        public async void should_delete_last()
        {
            await Store.DeleteAsync("delete_4", 3).ConfigureAwait(false);
            var acc = new Recorder();
            await Store.ReadPartitionForward("delete_4", 0, acc).ConfigureAwait(false);

            Assert.Equal(2, acc.Length);
            Assert.True((string)acc[0] == "1");
            Assert.True((string)acc[1] == "2");
        }

        [Fact]
        public async void should_delete_middle()
        {
            await Store.DeleteAsync("delete_5", 2, 2).ConfigureAwait(false);
            var acc = new Recorder();
            await Store.ReadPartitionForward("delete_5", 0, acc).ConfigureAwait(false);

            Assert.Equal(2, acc.Length);
            Assert.True((string)acc[0] == "1");
            Assert.True((string)acc[1] == "3");
        }
    }
    public class deleted_chunks_management : BasePersistenceTest
    {
        [Fact]
        public async void deleted_chunks_should_be_hidden_from_scan()
        {
            await Store.PersistAsync("a", 1, "first").ConfigureAwait(false);
            await Store.PersistAsync("a", 2, "second").ConfigureAwait(false);
            await Store.PersistAsync("a", 3, "third").ConfigureAwait(false);

            await Store.DeleteAsync("a", 2, 2).ConfigureAwait(false);

            var recorder = new AllPartitionsRecorder();
            await Store.ReadAllAsync(0, recorder).ConfigureAwait(false);

            Assert.Equal(2, recorder.Length);
            Assert.Equal("first", recorder[0]);
            Assert.Equal("third", recorder[1]);
        }

        [Fact]
        public async void deleted_chunks_should_be_hidden_from_peek()
        {
            await Store.PersistAsync("a", 1, "first").ConfigureAwait(false);
            await Store.PersistAsync("a", 2, "second").ConfigureAwait(false);

            await Store.DeleteAsync("a", 2, 2).ConfigureAwait(false);

            var chunk = await Store.ReadLast("a", 100, CancellationToken.None).ConfigureAwait(false);

            Assert.NotNull(chunk);
            Assert.Equal("first", chunk.Payload);
        }

        [Theory]
        [InlineData(1, 3)]
        [InlineData(2, 3)]
        //		[InlineData(3, 3)] @@TODO enable tombstone!
        public async void poller_should_skip_missing_chunks(long missing, long expected)
        {
            await Store.PersistAsync("a", 1, "1").ConfigureAwait(false);
            await Store.PersistAsync("a", 2, "2").ConfigureAwait(false);
            await Store.PersistAsync("a", 3, "3").ConfigureAwait(false);

            await Store.DeleteAsync("a", missing, missing).ConfigureAwait(false);

            var recored = new AllPartitionsRecorder();
            var poller = new PollingClient(Store, recored, this.LoggerFactory)
            {
                HoleDetectionTimeout = 100
            };

            var cts = new CancellationTokenSource(20000);

            await poller.Poll(cts.Token).ConfigureAwait(false);
            await poller.Poll(cts.Token).ConfigureAwait(false);
            Assert.Equal(expected, poller.Position);
        }
    }


    public class strict_sequence_on_store : BasePersistenceTest
    {
        [Fact]
        public async void on_concurrency_exception_holes_are_filled_with_empty_chunks()
        {
            if (!Store.SupportsFillers)
            {
                return;
            }

            var exceptions = 0;
            var writers = Enumerable.Range(1, 400).Select(async i =>
                {
                    try
                    {
                        await Store.PersistAsync("collision_wanted", 1 + i % 5, "payload").ConfigureAwait(false);
                    }
                    catch (DuplicateStreamIndexException)
                    {
                        Interlocked.Increment(ref exceptions);
                    }
                }
            ).ToArray();

            await Task.WhenAll(writers).ConfigureAwait(false);

            Assert.True(exceptions > 0);
            var recorder = new Recorder();
            await Store.ReadPartitionForward("::empty", 0, recorder).ConfigureAwait(false);

            Assert.Equal(exceptions, recorder.Length);
        }
    }

    public class concurrency_test : BasePersistenceTest
    {
        [Theory]
        [InlineData(1, false)]
        [InlineData(8, false)]
        [InlineData(1, true)]
        [InlineData(8, true)]
        public async void polling_client_should_not_miss_data(int parallelism, bool autopolling)
        {
            _logger.LogDebug("Starting with {Parallelism} workers and Autopolling {Autopolling}", parallelism, autopolling);

            var sequenceChecker = new StrictSequenceChecker($"Workers {parallelism} autopolling {autopolling}");
            var poller = new PollingClient(Store, sequenceChecker, this.LoggerFactory)
            {
                PollingIntervalMilliseconds = 0,
                HoleDetectionTimeout = 1000
            };

            if (autopolling)
            {
                poller.Start();
                _logger.LogDebug("Started Polling");
            }

            const int range = 1000;

            var producer = new ActionBlock<int>(async i =>
            {
                await Store.PersistAsync("p", -1, "demo").ConfigureAwait(false);
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = parallelism
            });

            _logger.LogDebug("Started pushing data: {elements} elements", range);

            foreach (var i in Enumerable.Range(1, range))
            {
                await producer.SendAsync(i).ConfigureAwait(false);
            }

            producer.Complete();
            await producer.Completion.ConfigureAwait(false);
            _logger.LogDebug("Data pushed");

            if (autopolling)
            {
                _logger.LogDebug("Stopping poller");
                poller.Stop();
                _logger.LogDebug("Poller stopped");
            }

            // read to end
            _logger.LogDebug("Polling to end");
            var timeout = new CancellationTokenSource(60000);
            await poller.Poll(timeout.Token).ConfigureAwait(false);
            _logger.LogDebug("Polling to end - done");

            Assert.Equal(range, poller.Position);
            Assert.Equal(range, sequenceChecker.Position);
        }
    }

    public class StrictSequenceChecker : ISubscription
    {
        private int _expectedPosition = 1;
        private readonly string _configMessage;

        public StrictSequenceChecker(string configMessage)
        {
            this._configMessage = configMessage;
        }

        public int Position => _expectedPosition - 1;

        public Task OnStart(long position)
        {
            return Task.CompletedTask;
        }

        public Task<bool> OnNext(IChunk data)
        {
            if (_expectedPosition != data.Position)
            {
                throw new Exception($"Expected position {_expectedPosition} got {data.Position} | {_configMessage}");
            }

            _expectedPosition++;
            return Task.FromResult(true);
        }

        public Task Completed(long position)
        {
            return Task.CompletedTask;
        }

        public Task Stopped(long position)
        {
            return Task.CompletedTask;
        }

        public Task OnError(long position, Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            throw ex;
        }
    }
}