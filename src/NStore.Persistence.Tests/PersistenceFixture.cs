using NStore.Core.Logging;
using NStore.Core.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Xunit;

// ReSharper disable InconsistentNaming
namespace NStore.Persistence.Tests
{
    public class TestMisconfiguredException : Exception
    {
        public TestMisconfiguredException(string message) : base(message)
        {
        }

        public TestMisconfiguredException() : base()
        {
        }

        public TestMisconfiguredException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    public abstract partial class BasePersistenceTest : IDisposable
    {
        private static int _staticId = 0;
        protected readonly int _testRunId;

        protected IPersistence Store { get; }
        protected readonly TestLoggerFactory LoggerFactory;
        protected readonly INStoreLogger _logger;
        protected IEnhancedPersistence Batcher => _persistence as IEnhancedPersistence;
        protected readonly IPersistence _persistence;

        protected BasePersistenceTest(bool autoCreateStore = true)
        {
            _testRunId = Interlocked.Increment(ref _staticId);

            LoggerFactory = new TestLoggerFactory(TestSuitePrefix + "::" + GetType().Name);
            _logger = LoggerFactory.CreateLogger(GetType().FullName);

            if (autoCreateStore)
            {
                _logger.LogDebug("Creating store");
                _persistence = Create(true);
                _logger.LogDebug("Store created");
                Store = new LogDecorator(_persistence, LoggerFactory);
            }
        }

        protected ISubscription EmptySubscription => new LambdaSubscription(_ => Task.FromResult(true));

        #region IDisposable Support

        private bool _disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (_disposedValue)
            {
                return;
            }

            if (disposing)
            {
                if (_persistence != null)
                {
                    Clear(_persistence, true);
                }

                _logger.LogDebug("Test disposed");
            }

            _disposedValue = true;
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }

    public class WriteTests : BasePersistenceTest
    {
        [Fact]
        public async Task can_insert_at_first_index()
        {
            var chunk = await Store.AppendAsync(
                "Stream_1",
                1,
                new { data = "this is a test" }
            ).ConfigureAwait(false);

            Assert.NotNull(chunk);
            Assert.Equal(1, chunk.Index);
            Assert.Equal("Stream_1", chunk.PartitionId);
        }
    }

    public class trying_to_persist_with_negative_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_throw()
        {
            var ex = await Assert.ThrowsAsync<InvalidStreamIndexException>(async () =>
            {
                await Store.AppendAsync("Stream_Neg", -1, "payload").ConfigureAwait(false);
            });

            Assert.Equal("Stream_Neg", ex.StreamId);
            Assert.Equal(-1, ex.StreamIndex);
        }
    }

    public class insert_at_last_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_work()
        {
            var chunk = await Store.AppendAsync(
                "Stream_1",
                long.MaxValue,
                new { data = "this is a test" }
            ).ConfigureAwait(false);

            Assert.NotNull(chunk);
            Assert.Equal(long.MaxValue, chunk.Index);
            Assert.Equal("Stream_1", chunk.PartitionId);
        }
    }

    public class insert_duplicate_chunk_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_throw()
        {
            await Store.AppendAsync("dup", 1, new { data = "first attempt" }).ConfigureAwait(false);
            await Store.AppendAsync("dup", 2, new { data = "should not work" }).ConfigureAwait(false);

            var ex = await Assert.ThrowsAnyAsync<DuplicateStreamIndexException>(() =>
                Store.AppendAsync("dup", 1, new { data = "this is a test" })
            ).ConfigureAwait(false);

            Assert.Equal("Duplicated index 1 on stream dup", ex.Message);
            Assert.Equal("dup", ex.StreamId);
            Assert.Equal(1, ex.StreamIndex);
        }
    }

    public class query_by_operation_id : BasePersistenceTest
    {
        public query_by_operation_id() : base()
        {
            try
            {
                BuildStream().ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                _logger.LogError("Test setup failed: {message}", e.Message);
                throw;
            }
        }

        [Fact]
        public async Task should_return_null_on_missing_operation()
        {
            var chunk = await Store.ReadByOperationIdAsync("stream_1", "nop", CancellationToken.None)
                .ConfigureAwait(false);
            Assert.Null(chunk);
        }

        [Fact]
        public async Task should_return_index_1_for_operation_1()
        {
            var chunk = await Store.ReadByOperationIdAsync("stream_1", "operation_1", CancellationToken.None)
                .ConfigureAwait(false);
            Assert.NotNull(chunk);
            Assert.Equal(1, chunk.Index);
        }

        [Fact]
        public async Task should_return_index_2_for_operation_2()
        {
            var chunk = await Store.ReadByOperationIdAsync("stream_1", "operation_2", CancellationToken.None)
                .ConfigureAwait(false);
            Assert.NotNull(chunk);
            Assert.Equal(2, chunk.Index);
        }

        [Fact]
        public async Task should_find_operation_on_stream_1_2_3()
        {
            var recorder = new Recorder();
            await Store.ReadAllByOperationIdAsync("operation_1", recorder, CancellationToken.None)
                .ConfigureAwait(false);

            Assert.Collection(recorder.Chunks,
                chunk => Assert.Equal("p1", chunk.Payload),
                chunk => Assert.Equal("p3", chunk.Payload),
                chunk => Assert.Equal("p4", chunk.Payload)
            );
        }

        private async Task BuildStream()
        {
            await Store.AppendAsync("stream_1", 1, "p1", "operation_1").ConfigureAwait(false);
            await Store.AppendAsync("stream_1", 2, "p2", "operation_2").ConfigureAwait(false);
            await Store.AppendAsync("stream_2", 1, "p3", "operation_1").ConfigureAwait(false);
            await Store.AppendAsync("stream_3", 1, "p4", "operation_1").ConfigureAwait(false);
            await Store.AppendAsync("stream_4", 1, "p5", "operation_2").ConfigureAwait(false);
        }
    }

    public class ScanTest : BasePersistenceTest
    {
        public ScanTest() : base()
        {
            try
            {
                Store.AppendAsync("Stream_1", 1, "a").ConfigureAwait(false).GetAwaiter().GetResult();
                Store.AppendAsync("Stream_1", 2, "b").ConfigureAwait(false).GetAwaiter().GetResult();
                Store.AppendAsync("Stream_1", 3, "c").ConfigureAwait(false).GetAwaiter().GetResult();

                Store.AppendAsync("Stream_2", 1, "d").ConfigureAwait(false).GetAwaiter().GetResult();
                Store.AppendAsync("Stream_2", 2, "e").ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                _logger.LogError("Scan test setup failed: {message}", e.Message);
                throw;
            }

            _logger.LogDebug("Scan test data written");
        }

        [Fact]
        public async Task ReadFirst()
        {
            object payload = null;

            await Store.ReadForwardAsync(
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

            await Store.ReadBackwardAsync(
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

            await Store.ReadForwardAsync(
                "Stream_1", 0, recorder, 2
            ).ConfigureAwait(false);

            Assert.Equal(2, recorder.Length);
            Assert.Equal("a", recorder[0].Payload);
            Assert.Equal("b", recorder[1].Payload);
        }

        [Fact]
        public async Task read_forward_should_call_complete_on_consumer()
        {
            var recorder = new Recorder();

            await Store.ReadForwardAsync(
                "Stream_1", 0, recorder, 2
            ).ConfigureAwait(false);

            Assert.True(recorder.ReadCompleted);
        }

        [Fact]
        public async Task read_backward_should_call_complete_on_consumer()
        {
            var recorder = new Recorder();

            await Store.ReadBackwardAsync(
                "Stream_1", 2, recorder, 0
            ).ConfigureAwait(false);

            Assert.True(recorder.ReadCompleted);
        }

        [Fact]
        public async Task should_read_only_last_two_chunks()
        {
            var tape = new Recorder();

            await Store.ReadBackwardAsync(
                "Stream_1",
                3,
                tape,
                2
            ).ConfigureAwait(false);

            Assert.Equal(2, tape.Length);
            Assert.Equal("c", tape[0].Payload);
            Assert.Equal("b", tape[1].Payload);
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

    public class read_last_position : BasePersistenceTest
    {
        [Fact]
        public async Task on_empty_store_should_be_equal_zero()
        {
            var last = await Store.ReadLastPositionAsync().ConfigureAwait(false);
            Assert.Equal(0L, last);
        }

        [Fact]
        public async Task with_only_one_chunk_should_be_equal_to_one()
        {
            await Store.AppendAsync("a", 1, "last").ConfigureAwait(false);
            var last = await Store.ReadLastPositionAsync().ConfigureAwait(false);
            Assert.Equal(1L, last);
        }

        [Fact]
        public async Task with_two_streams_of_one_chunk_should_be_two()
        {
            await Store.AppendAsync("a", 1, "first").ConfigureAwait(false);
            await Store.AppendAsync("b", 1, "second").ConfigureAwait(false);
            var last = await Store.ReadLastPositionAsync().ConfigureAwait(false);
            Assert.Equal(2L, last);
        }
    }

    public class ByteArrayPersistenceTest : BasePersistenceTest
    {
        [Fact]
        public async Task InsertByteArray()
        {
            await Store.AppendAsync("BA", 0, System.Text.Encoding.UTF8.GetBytes("this is a test"))
                .ConfigureAwait(false);

            byte[] payload = null;
            await Store.ReadForwardAsync("BA", 0, new LambdaSubscription(data =>
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
            await Store.AppendAsync("Id_1", 0, new { data = "this is a test" }, opId).ConfigureAwait(false);
            await Store.AppendAsync("Id_1", 1, new { data = "this is a test" }, opId).ConfigureAwait(false);

            var list = new List<long>();
            await Store.ReadForwardAsync("Id_1", 0, new LambdaSubscription(data =>
            {
                list.Add(data.Index);
                return Task.FromResult(true);
            })).ConfigureAwait(false);

            Assert.True(1 == list.Count);
            Assert.Equal(0, list[0]);
        }

        [Fact]
        public async Task can_append_same_operation_to_two_streams()
        {
            var opId = "operation_2";
            await Store.AppendAsync("Id_1", 0, "a", opId).ConfigureAwait(false);
            await Store.AppendAsync("Id_2", 1, "b", opId).ConfigureAwait(false);

            var list = new List<object>();
            await Store.ReadForwardAsync("Id_1", 0, new LambdaSubscription(data =>
            {
                list.Add(data.Payload);
                return Task.FromResult(true);
            })).ConfigureAwait(false);

            await Store.ReadForwardAsync("Id_2", 0, new LambdaSubscription(data =>
            {
                list.Add(data.Payload);
                return Task.FromResult(true);
            })).ConfigureAwait(false);

            Assert.Equal(2, list.Count);
        }
    }

    public class DeleteStreamTest : BasePersistenceTest
    {
        protected DeleteStreamTest() : base()
        {
            try
            {
                Task.WhenAll
                (
                    Store.AppendAsync("delete", 1, null),
                    Store.AppendAsync("delete_3", 1, "1"),
                    Store.AppendAsync("delete_3", 2, "2"),
                    Store.AppendAsync("delete_3", 3, "3"),
                    Store.AppendAsync("delete_4", 1, "1"),
                    Store.AppendAsync("delete_4", 2, "2"),
                    Store.AppendAsync("delete_4", 3, "3"),
                    Store.AppendAsync("delete_5", 1, "1"),
                    Store.AppendAsync("delete_5", 2, "2"),
                    Store.AppendAsync("delete_5", 3, "3")
                ).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                _logger.LogError("Delete stream test setup: {message}", e.Message);
                throw;
            }

            _logger.LogDebug("Delete test data written");
        }
    }

    public class DeleteStreamTest_1 : DeleteStreamTest
    {
        [Fact]
        public async Task delete_stream()
        {
            await Store.DeleteAsync("delete").ConfigureAwait(false);
            bool almostOneChunk = false;
            await Store.ReadForwardAsync("delete", 0, new LambdaSubscription(data =>
            {
                almostOneChunk = true;
                return Task.FromResult(false);
            })).ConfigureAwait(false);

            Assert.False(almostOneChunk, "Should not contains chunks");
        }
    }

    public class DeleteStreamTest_2 : DeleteStreamTest
    {
        [Fact]
        public async Task delete_invalid_stream_should_not_throw_exception()
        {
            await Store.DeleteAsync("delete_2").ConfigureAwait(false);
            Assert.True(true, "DeleteAsync should be idempotent and ignore deleted streams");
        }
    }

    public class DeleteStreamTest_3 : DeleteStreamTest
    {
        [Fact]
        public async Task should_delete_first()
        {
            _logger.LogDebug("deleting first chunk");
            await Store.DeleteAsync("delete_3", 1, 1).ConfigureAwait(false);
            _logger.LogDebug("recording");
            var acc = new Recorder();
            await Store.ReadForwardAsync("delete_3", 0, acc).ConfigureAwait(false);

            _logger.LogDebug("checking assertions");
            Assert.Equal(2, acc.Length);
            Assert.True((string)acc[0].Payload == "2");
            Assert.True((string)acc[1].Payload == "3");
            _logger.LogDebug("done");
        }
    }

    public class DeleteStreamTest_4 : DeleteStreamTest
    {
        [Fact]
        public async Task should_delete_last()
        {
            await Store.DeleteAsync("delete_4", 3).ConfigureAwait(false);
            var acc = new Recorder();
            await Store.ReadForwardAsync("delete_4", 0, acc).ConfigureAwait(false);

            Assert.Equal(2, acc.Length);
            Assert.True((string)acc[0].Payload == "1");
            Assert.True((string)acc[1].Payload == "2");
        }
    }

    public class DeleteStreamTest_5 : DeleteStreamTest
    {
        [Fact]
        public async Task should_delete_middle()
        {
            await Store.DeleteAsync("delete_5", 2, 2).ConfigureAwait(false);
            var acc = new Recorder();
            await Store.ReadForwardAsync("delete_5", 0, acc).ConfigureAwait(false);

            Assert.Equal(2, acc.Length);
            Assert.True((string)acc[0].Payload == "1");
            Assert.True((string)acc[1].Payload == "3");
        }
    }

    public class deleted_chunks_management : BasePersistenceTest
    {
        [Fact]
        public async Task deleted_chunks_should_be_hidden_from_scan()
        {
            await Store.AppendAsync("a", 1, "first").ConfigureAwait(false);
            await Store.AppendAsync("a", 2, "second").ConfigureAwait(false);
            await Store.AppendAsync("a", 3, "third").ConfigureAwait(false);

            await Store.DeleteAsync("a", 2, 2).ConfigureAwait(false);

            var recorder = new AllPartitionsRecorder();
            await Store.ReadAllAsync(0, recorder).ConfigureAwait(false);

            Assert.Equal(2, recorder.Length);
            Assert.Equal("first", recorder[0]);
            Assert.Equal("third", recorder[1]);
        }

        [Fact]
        public async Task deleted_chunks_should_be_hidden_from_peek()
        {
            await Store.AppendAsync("a", 1, "first").ConfigureAwait(false);
            await Store.AppendAsync("a", 2, "second").ConfigureAwait(false);

            await Store.DeleteAsync("a", 2, 2).ConfigureAwait(false);

            var chunk = await Store.ReadSingleBackwardAsync("a", 100, CancellationToken.None).ConfigureAwait(false);

            Assert.NotNull(chunk);
            Assert.Equal("first", chunk.Payload);
        }

        [Theory]
        [InlineData(1, 3)]
        [InlineData(2, 3)]
        //		[InlineData(3, 3)] @@TODO enable tombstone!
        public async Task poller_should_skip_missing_chunks(long missing, long expected)
        {
            await Store.AppendAsync("a", 1, "1").ConfigureAwait(false);
            await Store.AppendAsync("a", 2, "2").ConfigureAwait(false);
            await Store.AppendAsync("a", 3, "3").ConfigureAwait(false);

            await Store.DeleteAsync("a", missing, missing).ConfigureAwait(false);

            var recored = new AllPartitionsRecorder();
            var poller = new PollingClient(Store, 0, recored, this.LoggerFactory)
            {
                HoleDetectionTimeout = 100
            };

            var cts = new CancellationTokenSource(20000);

            await poller.Poll(cts.Token).ConfigureAwait(false);
            await poller.Poll(cts.Token).ConfigureAwait(false);
            Assert.Equal(expected, poller.Position);
        }
    }

    public class subscription_events_should_be_signaled : BasePersistenceTest
    {
        private readonly ChunkProcessor _continueToEnd = _ => Task.FromResult(true);
        private readonly LambdaSubscription _subscription;
        private long _startedAt = -1;
        private long _completedAt = -1;

        public subscription_events_should_be_signaled() : base()
        {
            // write stream out of order to have 
            // position 1 : index 2 
            // position 2 : index 1
            //
            Store.AppendAsync("a", 2, "data b").GetAwaiter().GetResult();
            Store.AppendAsync("a", 1, "data a").GetAwaiter().GetResult();

            _subscription = new LambdaSubscription(_continueToEnd)
            {
                OnStart = p =>
                {
                    _startedAt = p;
                    return Task.CompletedTask;
                },
                OnComplete = p =>
                {
                    _completedAt = p;
                    return Task.CompletedTask;
                }
            };
        }

        [Fact]
        public async Task on_read_all()
        {
            await Store.ReadAllAsync(fromPositionInclusive: 1, subscription: _subscription).ConfigureAwait(false);
            Assert.True(1 == _startedAt, "start position " + _startedAt);
            Assert.True(2 == _completedAt, "complete position " + _completedAt);
        }

        [Fact]
        public async Task on_read_forward()
        {
            await Store.ReadForwardAsync("a", fromLowerIndexInclusive: 1, subscription: _subscription)
                .ConfigureAwait(false);
            Assert.True(1 == _startedAt, _testRunId + ": start position " + _startedAt);
            Assert.True(2 == _completedAt, _testRunId + ": complete position " + _completedAt);
        }

        [Fact]
        public async Task on_read_backward()
        {
            await Store.ReadBackwardAsync("a", fromUpperIndexInclusive: 100, subscription: _subscription)
                .ConfigureAwait(false);
            Assert.True(100 == _startedAt, _testRunId + ": start position " + _startedAt);
            Assert.True(1 == _completedAt, _testRunId + ": complete position " + _completedAt);
        }
    }

    public class exceptions_should_be_signaled : BasePersistenceTest
    {
        private readonly ChunkProcessor _throw = c => throw new TimeoutException();
        private readonly LambdaSubscription _subscription;

        public exceptions_should_be_signaled() : base()
        {
            // write stream out of order to have 
            // position 1 : index 2 
            // position 2 : index 1
            //
            Store.AppendAsync("a", 2, "data b").GetAwaiter().GetResult();
            Store.AppendAsync("a", 1, "data a").GetAwaiter().GetResult();

            _subscription = new LambdaSubscription(_throw);
        }

        [Fact]
        public async Task on_read_all()
        {
            await Store.ReadAllAsync(0, _subscription).ConfigureAwait(false);
            Assert.True(_subscription.Failed);
        }

        [Fact]
        public async Task on_read_forward()
        {
            await Store.ReadForwardAsync("a", 0, _subscription).ConfigureAwait(false);
            Assert.True(_subscription.Failed);
        }

        [Fact]
        public async Task on_read_backward()
        {
            await Store.ReadBackwardAsync("a", 100, _subscription).ConfigureAwait(false);
            Assert.True(_subscription.Failed);
        }
    }

    public class strict_sequence_on_store : BasePersistenceTest
    {
        [Fact]
        public async Task on_concurrency_exception_holes_are_filled_with_empty_chunks()
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
                        await Store.AppendAsync("collision_wanted", 1 + i % 5, "payload").ConfigureAwait(false);
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
            await Store.ReadForwardAsync("::empty", 0, recorder).ConfigureAwait(false);

            Assert.Equal(exceptions, recorder.Length);
        }
    }

    public class polling_client_tests : BasePersistenceTest
    {
        [Theory]
        [InlineData(0, 3)]
        [InlineData(1, 2)]
        [InlineData(2, 1)]
        [InlineData(3, 0)]
        [InlineData(4, 0)]
        public async Task should_read_from_position(long start, long expected)
        {
            await Store.AppendAsync("a", 1, "1").ConfigureAwait(false);
            await Store.AppendAsync("a", 2, "2").ConfigureAwait(false);
            await Store.AppendAsync("a", 3, "3").ConfigureAwait(false);

            var recorder = new AllPartitionsRecorder();
            var client = new PollingClient(Store, start, recorder, LoggerFactory);

            await client.Poll(5000).ConfigureAwait(false);

            Assert.Equal(expected, recorder.Length);
        }
    }

    public class large_payload_tests : BasePersistenceTest
    {
        [Fact]
        public async Task can_write_and_read_large_payload()
        {
            var payload = new byte[1_000_000];
            for (int c = 0; c < payload.Length; c++)
            {
                payload[c] = Convert.ToByte(c % 255);
            }

            await _persistence.AppendAsync("large_binary", 1, payload).ConfigureAwait(false);

            var chunk = await _persistence.ReadSingleBackwardAsync("large_binary").ConfigureAwait(false);
            var loadedBytes = (byte[])chunk.Payload;

            Assert.Equal(payload, loadedBytes);
        }
    }

    public class concurrency_test : BasePersistenceTest
    {
        [Theory]
        [InlineData(1, false)]
        [InlineData(8, false)]
        [InlineData(1, true)]
        [InlineData(8, true)]
        public async Task polling_client_should_not_miss_data(int parallelism, bool autopolling)
        {
            _logger.LogDebug("Starting with {Parallelism} workers and Autopolling {Autopolling}", parallelism,
                autopolling);

            var sequenceChecker = new StrictSequenceChecker($"Workers {parallelism} autopolling {autopolling}");
            var poller = new PollingClient(Store, 0, sequenceChecker, this.LoggerFactory)
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

            var producer = new ActionBlock<int>(
                async i => { await Store.AppendAsync("p", i, "demo", "op#" + i).ConfigureAwait(false); },
                new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = parallelism
                });

            _logger.LogDebug("Started pushing data: {elements} elements", range);

            foreach (var i in Enumerable.Range(1, range))
            {
                Assert.True(await producer.SendAsync(i).ConfigureAwait(false));
            }

            producer.Complete();
            await producer.Completion.ConfigureAwait(false);
            _logger.LogDebug("Data pushed");

            if (autopolling)
            {
                _logger.LogDebug("Stopping poller");
                await poller.Stop().ConfigureAwait(false);
                _logger.LogDebug("Poller stopped");
            }

            // read to end
            _logger.LogDebug("Polling to end");
            var timeout = new CancellationTokenSource(60000);
            await poller.Poll(timeout.Token).ConfigureAwait(false);
            _logger.LogDebug("Polling to end - done");

            Assert.True(poller.Position == sequenceChecker.Position,
                "Sequence " + sequenceChecker.Position + " != Position " + poller.Position);
            Assert.True(range == poller.Position, "Poller @" + poller.Position);
            Assert.True(range == sequenceChecker.Position, "Sequence @" + sequenceChecker.Position);
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

        public Task OnStartAsync(long indexOrPosition)
        {
            return Task.CompletedTask;
        }

        public Task<bool> OnNextAsync(IChunk chunk)
        {
            if (_expectedPosition != chunk.Position)
            {
                throw new Exception($"Expected position {_expectedPosition} got {chunk.Position} | {_configMessage}");
            }

            _expectedPosition++;
            return Task.FromResult(true);
        }

        public Task CompletedAsync(long indexOrPosition)
        {
            return Task.CompletedTask;
        }

        public Task StoppedAsync(long indexOrPosition)
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(long indexOrPosition, Exception ex)
        {
            Console.WriteLine($"Error [{_configMessage}]: {ex.Message}\n{ex.StackTrace}");
            throw ex;
        }
    }

    public class FindOneTests : BasePersistenceTest
    {
        [Fact]
        public async Task should_return_null_on_chunk_not_found()
        {
            var notFound = await Store.ReadOneAsync(1);
            Assert.Null(notFound);
        }

        [Fact]
        public async Task should_return_chunk_by_position()
        {
            var first = await Store.AppendAsync("s", 1, "payload");
            var second = await Store.AppendAsync("s", 2, "payload");
            var found = await Store.ReadOneAsync(second.Position);

            Assert.NotNull(found);
            Assert.Equal(second.Position, found.Position);
            Assert.Equal("s", found.PartitionId);
            Assert.Equal(2, found.Index);
        }

        [Fact]
        public async Task should_not_find_deleted_chunk()
        {
            var first = await Store.AppendAsync("s", 1, "payload");
            await Store.DeleteAsync("s", 1, 1);

            var notFound = await Store.ReadOneAsync(first.Position);

            Assert.Null(notFound);
        }
    }

    public class ReplaceTests : BasePersistenceTest
    {
        [Fact]
        public async Task should_replace_chunk()
        {
            var chunk = await Store.AppendAsync("a", 1, "payload", "op_1");
            var replaced = await Store.ReplaceOneAsync(chunk.Position, "b", 1, "new payload", "op_2", CancellationToken.None);
            var recorder = new AllPartitionsRecorder();

            await Store.ReadAllAsync(0, recorder);

            Assert.NotSame(chunk, replaced);

            Assert.Collection(recorder.Chunks, c =>
            {
                Assert.Equal(1, c.Position);
                Assert.Equal("b", c.PartitionId);
                Assert.Equal(1, c.Index);
                Assert.Equal("new payload", c.Payload as string);
                Assert.Equal("op_2", c.OperationId);
            });
        }

        [Fact]
        public async Task previous_chunk_should_not_be_found_on_original_partition()
        {
            var chunk = await Store.AppendAsync("a", 1, "payload", "op_1");
            var replaced = await Store.ReplaceOneAsync(chunk.Position, "b", 1, "new payload", "op_2", CancellationToken.None);
            var recorder = new Recorder();

            await Store.ReadForwardAsync("a", 0, recorder);

            Assert.True(recorder.IsEmpty);
        }

        [Fact]
        public async Task rewriting_partition_id_should_check_duplicate_index()
        {
            var chunk = await Store.AppendAsync("a", 1, "payload", "op_1");
            await Store.AppendAsync("b", 1, "payload", "op_2");

            var ex = await Assert.ThrowsAsync<DuplicateStreamIndexException>(async () =>
            {
                await Store.ReplaceOneAsync(chunk.Position, "b", 1, "new payload", "op_1", CancellationToken.None);
            });

            Assert.Equal("b", ex.StreamId);
            Assert.Equal(1, ex.StreamIndex);
        }

        [Fact]
        public async Task rewriting_partition_id_should_check_operation_id()
        {
            var chunk = await Store.AppendAsync("a", 1, "payload", "op_1");
            await Store.AppendAsync("b", 2, "payload", "op_2");

            var replaced = await Store.ReplaceOneAsync(chunk.Position, "b", 1, "new payload", "op_2", CancellationToken.None);
            var original = await Store.ReadOneAsync(chunk.Position);

            Assert.Null(replaced);

            Assert.NotNull(original);
            Assert.NotSame(chunk, original);
            Assert.Equal(chunk.Position, original.Position);
            Assert.Equal("a", original.PartitionId);
            Assert.Equal(1, original.Index);
            Assert.Equal("payload", original.Payload as string);
            Assert.Equal("op_1", original.OperationId);
        }
    }

    public class MultiPartitionBasicRead : BasePersistenceTest
    {
        public MultiPartitionBasicRead()
        {
            int seed = 0;
            Store.AppendAsync("mbpra", 1, "payload", $"op_{seed++}").Wait();
            Store.AppendAsync("mbprb", 1, "payload", $"op_{seed++}").Wait();
            Store.AppendAsync("mbpra", 2, "payload", $"op_{seed++}").Wait();
            Store.AppendAsync("mbpra", 3, "payload", $"op_{seed++}").Wait();
            Store.AppendAsync("mbprc", 1, "payload", $"op_{seed++}").Wait();
            Store.AppendAsync("mbprb", 2, "payload", $"op_{seed++}").Wait();
            Store.AppendAsync("mbprz", 1, "payload", $"op_{seed++}").Wait();
        }

        [Fact]
        public async Task read_multiple_partition()
        {
            var tape = new Recorder();
            await Store.ReadForwardMultiplePartitionsAsync(new[] { "mbpra", "mbprb" }, 1, tape, Int32.MaxValue, CancellationToken.None);

            AssertForBasicRead(tape, 5);
        }

        [Fact]
        public async Task read_no_partitions()
        {
            var tape = new Recorder();

            //Read empty list 
            await Store.ReadForwardMultiplePartitionsAsync(Array.Empty<string>(), 1, tape, Int32.MaxValue, CancellationToken.None);

            Assert.Empty(tape.Chunks);
        }

        [Fact]
        public async Task read_with_extensions()
        {
            var tape = new Recorder();
            await Store.ReadForwardMultiplePartitionsAsync(new[] { "mbpra", "mbprb" }, tape);

            AssertForBasicRead(tape, 5);
        }

        [Fact]
        public async Task read_multiple_partition_can_read_single_partition()
        {
            var tape = new Recorder();
            await Store.ReadForwardMultiplePartitionsAsync(new[] { "mbpra" }, 1, tape, Int32.MaxValue, CancellationToken.None);

            Assert.Equal(3, tape.Chunks.Count());
            //we could not assume ordering, but clearly b1, is less than b2.
            var chunks = tape.Chunks.ToArray();
            Dictionary<string, long> checker = new Dictionary<string, long>()
            {
                ["mbpra"] = 0,
            };

            foreach (var chunk in chunks)
            {
                //Verify that actual chunk is greater than the previous on same partition
                //then update the dictionary.
                Assert.True(chunk.Index > checker[chunk.PartitionId]);
                checker[chunk.PartitionId] = chunk.Index;
            }
        }

        [Fact]
        public async Task read_limit_version()
        {
            var tape = new Recorder();
            await Store.ReadForwardMultiplePartitionsAsync(new[] { "mbpra", "mbprb" }, 1, tape, 2, CancellationToken.None);

            AssertForBasicRead(tape, 4);
        }

        [Fact]
        public async Task read_not_from_first_version()
        {
            var tape = new Recorder();
            await Store.ReadForwardMultiplePartitionsAsync(new[] { "mbpra", "mbprb" }, 2, tape, 2, CancellationToken.None);

            AssertForBasicRead(tape, 2);
        }

        [Fact]
        public async Task read_non_exiting_partition()
        {
            var tape = new Recorder();
            await Store.ReadForwardMultiplePartitionsAsync(new[] { "mbpra", "does-not-exists" }, 2, tape, 2, CancellationToken.None);

            AssertForBasicRead(tape, 1);
        }

        [Fact]
        public async Task read_multiple_partition_can_read_no_partition()
        {
            var tape = new Recorder();
            await Store.ReadForwardMultiplePartitionsAsync(Array.Empty<string>(), 1, tape, Int32.MaxValue, CancellationToken.None);

            Assert.Empty(tape.Chunks);
        }

        private static void AssertForBasicRead(Recorder tape, int expectedCount)
        {
            Assert.Equal(expectedCount, tape.Chunks.Count());
            //we could not assume ordering, but clearly b1, is less than b2.
            var chunks = tape.Chunks.ToArray();
            Dictionary<string, long> checker = new Dictionary<string, long>()
            {
                ["mbpra"] = 0,
                ["mbprb"] = 0,
            };

            foreach (var chunk in chunks)
            {
                //Verify that actual chunk is greater than the previous on same partition
                //then update the dictionary.
                Assert.True(chunk.Index > checker[chunk.PartitionId]);
                checker[chunk.PartitionId] = chunk.Index;
            }
        }

#if NET8_0_OR_GREATER
        [Fact]
        public async Task read_multiple_partition_async_enumerable()
        {
            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsAsyncEnumerable(new[] { "mbpra", "mbprb" }, 1, Int32.MaxValue, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            AssertForBasicReadList(chunks, 5);
        }

        [Fact]
        public async Task read_no_partitions_async_enumerable()
        {
            var chunks = new List<IChunk>();

            //Read empty list
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsAsyncEnumerable(Array.Empty<string>(), 1, Int32.MaxValue, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            Assert.Empty(chunks);
        }

        [Fact]
        public async Task read_with_extensions_async_enumerable()
        {
            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsAsyncEnumerable(new[] { "mbpra", "mbprb" }, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            AssertForBasicReadList(chunks, 5);
        }

        [Fact]
        public async Task read_multiple_partition_can_read_single_partition_async_enumerable()
        {
            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsAsyncEnumerable(new[] { "mbpra" }, 1, Int32.MaxValue, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            Assert.Equal(3, chunks.Count);
            //we could not assume ordering, but clearly b1, is less than b2.
            Dictionary<string, long> checker = new Dictionary<string, long>()
            {
                ["mbpra"] = 0,
            };

            foreach (var chunk in chunks)
            {
                //Verify that actual chunk is greater than the previous on same partition
                //then update the dictionary.
                Assert.True(chunk.Index > checker[chunk.PartitionId]);
                checker[chunk.PartitionId] = chunk.Index;
            }
        }

        [Fact]
        public async Task read_limit_version_async_enumerable()
        {
            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsAsyncEnumerable(new[] { "mbpra", "mbprb" }, 1, 2, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            AssertForBasicReadList(chunks, 4);
        }

        [Fact]
        public async Task read_not_from_first_version_async_enumerable()
        {
            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsAsyncEnumerable(new[] { "mbpra", "mbprb" }, 2, 2, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            AssertForBasicReadList(chunks, 2);
        }

        [Fact]
        public async Task read_non_exiting_partition_async_enumerable()
        {
            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsAsyncEnumerable(new[] { "mbpra", "does-not-exists" }, 2, 2, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            AssertForBasicReadList(chunks, 1);
        }

        [Fact]
        public async Task read_multiple_partition_can_read_no_partition_async_enumerable()
        {
            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsAsyncEnumerable(Array.Empty<string>(), 1, Int32.MaxValue, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            Assert.Empty(chunks);
        }

        private static void AssertForBasicReadList(IEnumerable<IChunk> chunks, int expectedCount)
        {
            var chunksList = chunks.ToList();
            Assert.Equal(expectedCount, chunksList.Count);
            //we could not assume ordering, but clearly b1, is less than b2.
            Dictionary<string, long> checker = new Dictionary<string, long>()
            {
                ["mbpra"] = 0,
                ["mbprb"] = 0,
            };

            foreach (var chunk in chunksList)
            {
                //Verify that actual chunk is greater than the previous on same partition
                //then update the dictionary.
                Assert.True(chunk.Index > checker[chunk.PartitionId]);
                checker[chunk.PartitionId] = chunk.Index;
            }
        }
#endif
    }

    public class MultiPartitionRangesRead : BasePersistenceTest
    {
        public MultiPartitionRangesRead()
        {
            // Seed test data:
            // mbpra: indices 1, 2, 3
            // mbprb: indices 1, 2
            // mbprc: index 1
            int seed = 0;
            Store.AppendAsync("mbpra", 1, "payload_a1", $"op_{seed++}").Wait();
            Store.AppendAsync("mbprb", 1, "payload_b1", $"op_{seed++}").Wait();
            Store.AppendAsync("mbpra", 2, "payload_a2", $"op_{seed++}").Wait();
            Store.AppendAsync("mbpra", 3, "payload_a3", $"op_{seed++}").Wait();
            Store.AppendAsync("mbprc", 1, "payload_c1", $"op_{seed++}").Wait();
            Store.AppendAsync("mbprb", 2, "payload_b2", $"op_{seed++}").Wait();
        }

        [Fact]
        public async Task read_multiple_partitions_with_ranges_subscription()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 1, long.MaxValue),
                new PartitionReadRequest("mbprb", 1, long.MaxValue)
            };

            var recorder = new Recorder();
            await Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, recorder, CancellationToken.None);

            Assert.Equal(5, recorder.Chunks.Count());
            AssertPerPartitionOrdering(recorder.Chunks, "mbpra", "mbprb");
        }

        [Fact]
        public async Task read_specific_ranges_subscription()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 2, 3),
                new PartitionReadRequest("mbprb", 1, 1)
            };

            var recorder = new Recorder();
            await Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, recorder, CancellationToken.None);

            Assert.Equal(3, recorder.Chunks.Count());
            AssertPerPartitionOrdering(recorder.Chunks, "mbpra", "mbprb");

            // Verify we got the right chunks
            var mbpraChunks = recorder.Chunks.Where(c => c.PartitionId == "mbpra").ToList();
            var mbprbChunks = recorder.Chunks.Where(c => c.PartitionId == "mbprb").ToList();

            Assert.Equal(2, mbpraChunks.Count);
            Assert.All(mbpraChunks, c => Assert.InRange(c.Index, 2, 3));

            Assert.Single(mbprbChunks);
            Assert.Equal(1, mbprbChunks[0].Index);
        }

        [Fact]
        public async Task read_empty_partition_requests_subscription()
        {
            var requests = Array.Empty<PartitionReadRequest>();

            var recorder = new Recorder();
            await Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, recorder, CancellationToken.None);

            Assert.Empty(recorder.Chunks);
        }

        [Fact]
        public async Task read_non_existing_partition_subscription()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 2, 2),
                new PartitionReadRequest("does-not-exist", 1, long.MaxValue)
            };

            var recorder = new Recorder();
            await Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, recorder, CancellationToken.None);

            Assert.Single(recorder.Chunks);
            Assert.Equal("mbpra", recorder.Chunks.First().PartitionId);
            Assert.Equal(2, recorder.Chunks.First().Index);
        }

        [Fact]
        public async Task read_single_partition_with_range_subscription()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 1, 2)
            };

            var recorder = new Recorder();
            await Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, recorder, CancellationToken.None);

            Assert.Equal(2, recorder.Chunks.Count());
            Assert.All(recorder.Chunks, c => Assert.Equal("mbpra", c.PartitionId));
            AssertPerPartitionOrdering(recorder.Chunks, "mbpra");
        }

        [Fact]
        public async Task subscription_honors_stop_signal()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 1, long.MaxValue),
                new PartitionReadRequest("mbprb", 1, long.MaxValue)
            };

            var subscription = new StopAfterNSubscription(2);
            await Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, subscription, CancellationToken.None);

            Assert.True(subscription.Chunks.Count <= 2, "Should stop after receiving 2 chunks");
            Assert.True(subscription.WasStopped || subscription.WasCompleted, "Should have called Stopped or Completed");
        }

        [Fact]
        public async Task read_with_duplicate_partition_requests_subscription()
        {
            // Same partition with different ranges - should get union of ranges
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 1, 1),
                new PartitionReadRequest("mbpra", 3, 3)
            };

            var recorder = new Recorder();
            await Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, recorder, CancellationToken.None);

            var mbpraChunks = recorder.Chunks.Where(c => c.PartitionId == "mbpra").ToList();

            // Should get chunks from both ranges
            Assert.Contains(mbpraChunks, c => c.Index == 1);
            Assert.Contains(mbpraChunks, c => c.Index == 3);

            // Ensure no duplicates - each index should appear only once
            var indices = mbpraChunks.Select(c => c.Index).ToList();
            Assert.Equal(indices.Distinct().Count(), indices.Count);
        }

#if NET8_0_OR_GREATER
        [Fact]
        public async Task read_multiple_partitions_with_ranges_async_enumerable()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 1, long.MaxValue),
                new PartitionReadRequest("mbprb", 1, long.MaxValue)
            };

            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            Assert.Equal(5, chunks.Count);
            AssertPerPartitionOrdering(chunks, "mbpra", "mbprb");
        }

        [Fact]
        public async Task read_specific_ranges_async_enumerable()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 2, 3),
                new PartitionReadRequest("mbprb", 1, 1)
            };

            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            Assert.Equal(3, chunks.Count);
            AssertPerPartitionOrdering(chunks, "mbpra", "mbprb");

            var mbpraChunks = chunks.Where(c => c.PartitionId == "mbpra").ToList();
            var mbprbChunks = chunks.Where(c => c.PartitionId == "mbprb").ToList();

            Assert.Equal(2, mbpraChunks.Count);
            Assert.All(mbpraChunks, c => Assert.InRange(c.Index, 2, 3));

            Assert.Single(mbprbChunks);
            Assert.Equal(1, mbprbChunks[0].Index);
        }

        [Fact]
        public async Task read_empty_partition_requests_async_enumerable()
        {
            var requests = Array.Empty<PartitionReadRequest>();

            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            Assert.Empty(chunks);
        }

        [Fact]
        public async Task read_non_existing_partition_async_enumerable()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 2, 2),
                new PartitionReadRequest("does-not-exist", 1, long.MaxValue)
            };

            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            Assert.Single(chunks);
            Assert.Equal("mbpra", chunks[0].PartitionId);
            Assert.Equal(2, chunks[0].Index);
        }

        [Fact]
        public async Task read_single_partition_with_range_async_enumerable()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 1, 2)
            };

            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            Assert.Equal(2, chunks.Count);
            Assert.All(chunks, c => Assert.Equal("mbpra", c.PartitionId));
            AssertPerPartitionOrdering(chunks, "mbpra");
        }

        [Fact]
        public async Task async_enumerable_honors_early_break()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 1, long.MaxValue),
                new PartitionReadRequest("mbprb", 1, long.MaxValue)
            };

            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, CancellationToken.None))
            {
                chunks.Add(chunk);
                if (chunks.Count >= 2)
                    break;
            }

            Assert.Equal(2, chunks.Count);
        }

        [Fact]
        public async Task read_with_overlapping_ranges_different_partitions_async_enumerable()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 1, 3),
                new PartitionReadRequest("mbprb", 1, 2),
                new PartitionReadRequest("mbprc", 1, 1)
            };

            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            Assert.Equal(6, chunks.Count);
            AssertPerPartitionOrdering(chunks, "mbpra", "mbprb", "mbprc");
        }

        [Fact]
        public async Task read_with_duplicate_partition_requests_async_enumerable()
        {
            // Same partition with different ranges
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 1, 1),
                new PartitionReadRequest("mbpra", 3, 3)
            };

            var chunks = new List<IChunk>();
            await foreach (var chunk in Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, CancellationToken.None))
            {
                chunks.Add(chunk);
            }

            var mbpraChunks = chunks.Where(c => c.PartitionId == "mbpra").ToList();

            Assert.Contains(mbpraChunks, c => c.Index == 1);
            Assert.Contains(mbpraChunks, c => c.Index == 3);

            // Ensure no duplicates
            var indices = mbpraChunks.Select(c => c.Index).ToList();
            Assert.Equal(indices.Distinct().Count(), indices.Count);
        }

        [Fact]
        public async Task cancellation_token_cancels_enumeration()
        {
            var requests = new[]
            {
                new PartitionReadRequest("mbpra", 1, long.MaxValue),
                new PartitionReadRequest("mbprb", 1, long.MaxValue)
            };

            using var cts = new CancellationTokenSource();
            cts.Cancel(); // Cancel immediately

            var chunks = new List<IChunk>();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await foreach (var chunk in Store.ReadForwardMultiplePartitionsWithRangesAsync(requests, cts.Token))
                {
                    chunks.Add(chunk);
                }
            });
        }

        public class multi_stream_read_with_ranges : BasePersistenceTest
        {
            [Fact]
            public async Task read_100_partitions_with_varied_ranges_over_200_partitions()
            {
                // create 200 partitions each with 20 commits
                var allPartitions = Enumerable.Range(0, 200).Select(i => $"p-{i}").ToArray();

                foreach (var p in allPartitions)
                {
                    for (long idx = 1; idx <= 20; idx++)
                    {
                        await _persistence.AppendAsync(p, idx, new { partition = p, idx = idx }).ConfigureAwait(false);
                    }
                }

                // pick 100 partitions (every 2nd partition) to read with different ranges
                var selected = allPartitions.Where((_, i) => i % 2 == 0).Take(100).ToArray();

                var requests = new List<PartitionReadRequest>();
                for (int i = 0; i < selected.Length; i++)
                {
                    var p = selected[i];
                    // vary ranges across four patterns
                    var mod = i % 4;
                    PartitionReadRequest req;
                    switch (mod)
                    {
                        case 0: // full range
                            req = new PartitionReadRequest(p, 1, 20);
                            break;
                        case 1: // head-only (last 5)
                            req = new PartitionReadRequest(p, 16, 20);
                            break;
                        case 2: // tail-only (first 5)
                            req = new PartitionReadRequest(p, 1, 5);
                            break;
                        default: // mid-range
                            req = new PartitionReadRequest(p, 5, 15);
                            break;
                    }
                    requests.Add(req);
                }

                var recorder = new Recorder();

                // perform the multi-partition ranged read
                await _persistence.ReadForwardMultiplePartitionsWithRangesAsync(requests, recorder, CancellationToken.None).ConfigureAwait(false);

                // group results per partition and assert
                var groups = recorder.Chunks.GroupBy(c => c.PartitionId)
                    .ToDictionary(g => g.Key, g => g.OrderBy(c => c.Index).ToArray());

                foreach (var req in requests)
                {
                    Assert.True(groups.ContainsKey(req.PartitionId), $"Missing partition {req.PartitionId}");
                    var chunks = groups[req.PartitionId];

                    // each chunk index must be within requested range
                    foreach (var c in chunks)
                    {
                        Assert.InRange(c.Index, req.FromPartitionIndexInclusive, req.ToPartitionIndexInclusive);
                        // payload was stored as an anonymous object; validate expected fields
                        Assert.NotNull(c.Payload);
                    }

                    // verify expected count matches the request
                    var expectedCount = (int)(Math.Max(0, Math.Min(20, req.ToPartitionIndexInclusive) - Math.Max(1, req.FromPartitionIndexInclusive) + 1));
                    Assert.Equal(expectedCount, chunks.Length);
                }
            }
        }
#endif

        private static void AssertPerPartitionOrdering(IEnumerable<IChunk> chunks, params string[] partitions)
        {
            var map = partitions.ToDictionary(p => p, _ => 0L);

            foreach (var chunk in chunks)
            {
                if (map.ContainsKey(chunk.PartitionId))
                {
                    Assert.True(chunk.Index > map[chunk.PartitionId],
                        $"Partition {chunk.PartitionId}: chunk index {chunk.Index} is not greater than previous {map[chunk.PartitionId]}");
                    map[chunk.PartitionId] = chunk.Index;
                }
            }
        }

        private class StopAfterNSubscription : ISubscription
        {
            private readonly int _maxChunks;
            public List<IChunk> Chunks { get; } = new List<IChunk>();
            public bool WasStopped { get; private set; }
            public bool WasCompleted { get; private set; }

            public StopAfterNSubscription(int maxChunks)
            {
                _maxChunks = maxChunks;
            }

            public Task<bool> OnNextAsync(IChunk chunk)
            {
                Chunks.Add(chunk);
                return Task.FromResult(Chunks.Count < _maxChunks);
            }

            public Task OnStartAsync(long indexOrPosition)
            {
                return Task.CompletedTask;
            }

            public Task CompletedAsync(long indexOrPosition)
            {
                WasCompleted = true;
                return Task.CompletedTask;
            }

            public Task StoppedAsync(long indexOrPosition)
            {
                WasStopped = true;
                return Task.CompletedTask;
            }

            public Task OnErrorAsync(long indexOrPosition, Exception ex)
            {
                return Task.FromException(ex);
            }
        }
    }
}