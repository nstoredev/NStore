using System;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using NStore.Core.Streams;
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
        public async Task create_stream()
        {
            var stream = _streams.Open("stream_1");
            await stream.AppendAsync("payload").ConfigureAwait(false);

            var acc = new Recorder();
            await Store.ReadForwardAsync("stream_1", 0, acc).ConfigureAwait(false);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0].Payload);
        }

        [Fact]
        public async Task read_stream()
        {
            await Store.AppendAsync("stream_2", 1, "payload").ConfigureAwait(false);

            var stream = _streams.Open("stream_2");
            var acc = new Recorder();
            await stream.ReadAsync(acc).ConfigureAwait(false);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0].Payload);
        }

        [Fact]
        public async Task delete_stream()
        {
            await Store.AppendAsync("stream_3", 1, "payload").ConfigureAwait(false);
            var stream = _streams.Open("stream_3");
            await stream.DeleteAsync().ConfigureAwait(false);

            var acc = new Recorder();
            await stream.ReadAsync(acc).ConfigureAwait(false);

            Assert.True(acc.IsEmpty);
        }

        [Fact]
        public async Task is_empty()
        {
            var stream = _streams.Open("brand_new_stream");
            Assert.True(await stream.IsEmpty().ConfigureAwait(false));

            await stream.AppendAsync("a").ConfigureAwait(false);
            Assert.False(await stream.IsEmpty().ConfigureAwait(false));
        }

        [Fact]
        public async Task contains_operation_id()
        {
            await Store.AppendAsync("stream_3", 1, "payload", "operation_1").ConfigureAwait(false);
            var stream = _streams.Open("stream_3");
            var opFound = await stream.ContainsOperationAsync("operation_1").ConfigureAwait(false);
            Assert.True(opFound);
        }

        [Fact]
        public async Task should_not_contains_operation_id()
        {
            await Store.AppendAsync("stream_3", 1, "payload", "operation_1").ConfigureAwait(false);
            var stream = _streams.Open("stream_3");
            var opFound = await stream.ContainsOperationAsync("operation_2").ConfigureAwait(false);
            Assert.False(opFound);
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
                await stream.ReadAsync(NullSubscription.Instance).ConfigureAwait(false);
            }
            return stream;
        }

        [Fact]
        public async Task read_stream()
        {
            await Store.AppendAsync("stream_2", 1, "payload").ConfigureAwait(false);

            var stream = await Open("stream_2").ConfigureAwait(false);
            var acc = new Recorder();
            await stream.ReadAsync(acc).ConfigureAwait(false);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0].Payload);
        }

        [Fact]
        public async Task appending_first_chunk()
        {
            var stream = await Open("stream_1").ConfigureAwait(false);

            await stream.AppendAsync("payload").ConfigureAwait(false);
            var tape = new Recorder();
            await Store.ReadForwardAsync("stream_1", 0, tape).ConfigureAwait(false);

            Assert.Equal(1, tape.Length);
            Assert.Equal("payload", tape[0].Payload);
            Assert.Equal("payload", tape.ByIndex(1).Payload);
        }

        [Fact]
        public async Task appending_two_chunks_sequentially()
        {
            var stream = await Open("stream_1").ConfigureAwait(false);
            await stream.AppendAsync("a").ConfigureAwait(false);
            await stream.AppendAsync("b").ConfigureAwait(false);

            var tape = new Recorder();
            await Store.ReadForwardAsync("stream_1", 0, tape).ConfigureAwait(false);

            Assert.Equal(2, tape.Length);
            Assert.Equal("a", tape[0].Payload);
            Assert.Equal("a", tape.ByIndex(1).Payload);
            Assert.Equal("b", tape[1].Payload);
            Assert.Equal("b", tape.ByIndex(2).Payload);
        }

        [Fact]
        public async Task appending_two_chunks_at_same_version_should_throw_concurrency_exception()
        {
            var streama = await Open("stream_1").ConfigureAwait(false);
            var streamb = await Open("stream_1").ConfigureAwait(false);
            await streama.AppendAsync("a").ConfigureAwait(false);

            var ex = await Assert.ThrowsAnyAsync<ConcurrencyException>(() =>
                streamb.AppendAsync("b")
            ).ConfigureAwait(false);

            Assert.Equal("Concurrency exception on StreamId: stream_1", ex.Message);
            Assert.IsType<DuplicateStreamIndexException>(ex.InnerException);
        }

        [Fact]
        public async Task appending_on_a_stream_without_reading_to_end_should_throw()
        {
            var stream = await Open("stream_1", false).ConfigureAwait(false);
            var ex = await Assert.ThrowsAnyAsync<AppendFailedException>(() =>
                stream.AppendAsync("b")
            ).ConfigureAwait(false);
        }

        [Fact]
        public async Task appending_on_a_partially_loaded_stream_should_throw()
        {
            var stream = await Open("stream_1", false).ConfigureAwait(false);
            await stream.ReadAsync(NullSubscription.Instance, 0, 10).ConfigureAwait(false);
            var ex = await Assert.ThrowsAnyAsync<AppendFailedException>(() =>
                stream.AppendAsync("b")
            ).ConfigureAwait(false);
        }

        [Fact]
        public async Task is_empty()
        {
            var stream = _streams.Open("brand_new_stream");
            Assert.True(await stream.IsEmpty().ConfigureAwait(false));

            await stream.AppendAsync("a").ConfigureAwait(false);
            Assert.False(await stream.IsEmpty().ConfigureAwait(false));
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
        public async Task read_stream()
        {
            await Store.AppendAsync("stream_2", 1, "payload").ConfigureAwait(false);

            var stream = _streams.OpenReadOnly("stream_2");
            var acc = new Recorder();
            await stream.ReadAsync(acc).ConfigureAwait(false);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0].Payload);
        }

        [Fact]
        public async Task delete_should_throw_exception()
        {
            var stream = _streams.OpenReadOnly("stream_2");
            var ex = await Assert.ThrowsAsync<StreamReadOnlyException>(() =>
                stream.DeleteAsync()
            ).ConfigureAwait(false);
        }

        [Fact]
        public async Task append_should_throw_exception()
        {
            var stream = _streams.OpenReadOnly("stream_2");
            var ex = await Assert.ThrowsAsync<StreamReadOnlyException>(() =>
                stream.AppendAsync("a")
            ).ConfigureAwait(false);
        }

        [Fact]
        public async Task is_empty()
        {
            await Store.AppendAsync("stream_2", 1, "payload").ConfigureAwait(false);

            var stream = _streams.OpenReadOnly("stream_2");
            Assert.False(await stream.IsEmpty().ConfigureAwait(false));
        }

    }
}