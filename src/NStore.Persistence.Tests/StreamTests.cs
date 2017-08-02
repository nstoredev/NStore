using System;
using System.Threading.Tasks;
using NStore.Persistence;
using NStore.Streams;
using Xunit;

namespace NStore.Persistence.Tests
{
    public class StreamTests : BasePersistenceTest
    {
        private readonly IStreamsFactory _streams;

        public StreamTests()
        {
            _streams = new StreamsFactory(Store);
        }

        [Fact]
        public async void create_stream()
        {
            var stream = _streams.Open("stream_1");
            await stream.AppendAsync("payload");

            var acc = new Recorder();
            await Store.ReadPartitionForward("stream_1", 0, acc);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0]);
        }

        [Fact]
        public async void read_stream()
        {
            await Store.AppendAsync("stream_2", 1, "payload");

            var stream = _streams.Open("stream_2");
            var acc = new Recorder();
            await stream.Read(acc);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0]);
        }

        [Fact]
        public async void delete_stream()
        {
            await Store.AppendAsync("stream_3", 1, "payload");
            var stream = _streams.Open("stream_3");
            await stream.DeleteAsync();

            var acc = new Recorder();
            await stream.Read(acc);

            Assert.True(acc.IsEmpty);
        }
    }

    public class OptimisticConcurrencyStreamTests : BasePersistenceTest
    {
        private readonly IStreamsFactory _streams;

        public OptimisticConcurrencyStreamTests()
        {
            _streams = new StreamsFactory(Store);
        }

        private async Task<IStream> Open(string id, bool readToEnd = true)
        {
            var stream = _streams.OpenOptimisticConcurrency(id);
            if (readToEnd)
            {
                await stream.Read(NullSubscription.Instance).ConfigureAwait(false);
            }
            return stream;
        }

        [Fact]
        public async void read_stream()
        {
            await Store.AppendAsync("stream_2", 1, "payload").ConfigureAwait(false);

            var stream = await Open("stream_2").ConfigureAwait(false);
            var acc = new Recorder();
            await stream.Read(acc).ConfigureAwait(false);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0]);
        }

        [Fact]
        public async void appending_first_chunk()
        {
            var stream = await Open("stream_1").ConfigureAwait(false);

            await stream.AppendAsync("payload").ConfigureAwait(false);
            var tape = new Recorder();
            await Store.ReadPartitionForward("stream_1", 0, tape).ConfigureAwait(false);

            Assert.Equal(1, tape.Length);
            Assert.Equal("payload", tape[0]);
            Assert.Equal("payload", tape.ByIndex(1));
        }

        [Fact]
        public async void appending_two_chunks_sequentially()
        {
            var stream = await Open("stream_1").ConfigureAwait(false);
            await stream.AppendAsync("a").ConfigureAwait(false);
            await stream.AppendAsync("b").ConfigureAwait(false);

            var tape = new Recorder();
            await Store.ReadPartitionForward("stream_1", 0, tape).ConfigureAwait(false);

            Assert.Equal(2, tape.Length);
            Assert.Equal("a", tape[0]);
            Assert.Equal("a", tape.ByIndex(1));
            Assert.Equal("b", tape[1]);
            Assert.Equal("b", tape.ByIndex(2));
        }

        [Fact]
        public async void appending_two_chunks_at_same_version_should_throw_concurrency_exception()
        {
            var streama = await Open("stream_1").ConfigureAwait(false);
            var streamb = await Open("stream_1").ConfigureAwait(false);
            await streama.AppendAsync("a").ConfigureAwait(false);

            var ex = await Assert.ThrowsAnyAsync<DuplicateStreamIndexException>(() =>
                streamb.AppendAsync("b")
            ).ConfigureAwait(false);

            Assert.Equal(1, ex.Index);
        }

        [Fact]
        public async void appending_on_a_stream_without_reading_to_end_should_throw()
        {
            var stream = await Open("stream_1", false).ConfigureAwait(false);
            var ex = await Assert.ThrowsAnyAsync<AppendFailedException>(() =>
                stream.AppendAsync("b")
            ).ConfigureAwait(false);
        }

        [Fact]
        public async void appending_on_a_partially_loaded_stream_should_throw()
        {
            var stream = await Open("stream_1", false).ConfigureAwait(false);
            await stream.Read(NullSubscription.Instance, 0, 10).ConfigureAwait(false);
            var ex = await Assert.ThrowsAnyAsync<AppendFailedException>(() =>
                stream.AppendAsync("b")
            ).ConfigureAwait(false);
        }
    }

    public class ReadOnlyStreamTests : BasePersistenceTest
    {
        private readonly IStreamsFactory _streams;

        public ReadOnlyStreamTests()
        {
            _streams = new StreamsFactory(Store);
        }

        [Fact]
        public async void read_stream()
        {
            await Store.AppendAsync("stream_2", 1, "payload").ConfigureAwait(false);

            var stream = _streams.OpenReadOnly("stream_2");
            var acc = new Recorder();
            await stream.Read(acc).ConfigureAwait(false);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0]);
        }

        [Fact]
        public async void delete_should_throw_exception()
        {
            var stream = _streams.OpenReadOnly("stream_2");
            var ex = await Assert.ThrowsAsync<StreamReadOnlyException>(() =>
                stream.DeleteAsync()
            ).ConfigureAwait(false);
        }

        [Fact]
        public async void append_should_throw_exception()
        {
            var stream = _streams.OpenReadOnly("stream_2");
            var ex = await Assert.ThrowsAsync<StreamReadOnlyException>(() =>
                stream.AppendAsync("a")
            ).ConfigureAwait(false);
        }
    }
}