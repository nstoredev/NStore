using System;
using NStore.Raw;
using NStore.Streams;
using Xunit;

namespace NStore.Persistence.Tests
{
    public class StreamTests : BasePersistenceTest
    {
        private readonly IStreamStore _streams;

        public StreamTests()
        {
            _streams = new StreamStore(Store);
        }

        [Fact]
        public async void create_stream()
        {
            var stream = _streams.Open("stream_1");
            await stream.Append("payload");

            var acc = new Tape();
            await Store.ScanPartitionAsync("stream_1", 0, ScanDirection.Forward, acc.Record);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0]);
        }

        [Fact]
        public async void read_stream()
        {
            await Store.PersistAsync("stream_2", 1, "payload");

            var stream = _streams.Open("stream_2");
            var acc = new Tape();
            await stream.Read(0, Int32.MaxValue, acc.Record);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0]);
        }

        [Fact]
        public async void delete_stream()
        {
            await Store.PersistAsync("stream_3", 1, "payload");
            var stream = _streams.Open("stream_3");
            await stream.Delete();

            var acc = new Tape();
            await stream.Read(0, Int32.MaxValue, acc.Record);

            Assert.True(acc.IsEmpty);
        }
    }

    public class OptimisticConcurrencyStreamTests : BasePersistenceTest
    {
        private readonly IStreamStore _streams;

        public OptimisticConcurrencyStreamTests()
        {
            _streams = new StreamStore(Store);
        }

        [Fact]
        public async void appending_first_chunk()
        {
            var stream = _streams.OpenOptimisticConcurrency("stream_1");
            await stream.Append(1, "payload");
            var tape = new Tape();
            await Store.ScanPartitionAsync("stream_1", 0, ScanDirection.Forward, tape.Record);

            Assert.Equal(1, tape.Length);
            Assert.Equal("payload", tape[0]);
            Assert.Equal("payload", tape.ByIndex(1));
        }

        [Fact]
        public async void appending_two_chunks_sequentially()
        {
            var stream = _streams.OpenOptimisticConcurrency("stream_1");
            await stream.Append(1, "a");
            await stream.Append(2, "b");

            var tape = new Tape();
            await Store.ScanPartitionAsync("stream_1", 0, ScanDirection.Forward, tape.Record);

            Assert.Equal(2, tape.Length);
            Assert.Equal("a", tape[0]);
            Assert.Equal("a", tape.ByIndex(1));
            Assert.Equal("b", tape[1]);
            Assert.Equal("b", tape.ByIndex(2));
        }

        [Fact]
        public async void appending_two_chunks_at_same_version_should_throw_concurrency_exception()
        {
            var stream = _streams.OpenOptimisticConcurrency("stream_1");
            await stream.Append(1, "a");

            var ex = await Assert.ThrowsAnyAsync<DuplicateStreamIndexException>(() =>
                stream.Append(1, "b")
            );

            Assert.Equal(1, ex.Index);
        }
    }
}