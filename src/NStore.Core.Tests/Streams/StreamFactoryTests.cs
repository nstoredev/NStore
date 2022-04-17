using NStore.Core.InMemory;
using NStore.Core.Streams;
using Xunit;

namespace NStore.Core.Tests.Streams
{
    public class StreamFactoryTests
    {
        private readonly IStreamsFactory _store = new StreamsFactory(new InMemoryStore(new InMemoryPersistenceOptions()));

        [Fact]
        public void open_should_return_stream()
        {
            var stream = _store.Open("id");
            Assert.IsType<Stream>(stream);
            Assert.True(stream.IsWritable);
        }

        [Fact]
        public void open_optimistic_should_return_optimistic_concurrency_enabled_stream()
        {
            var stream = _store.OpenOptimisticConcurrency("id");
            Assert.IsType<OptimisticConcurrencyStream>(stream);
            Assert.True(stream.IsWritable);
        }

        [Fact]
        public void open_readonly_should_return_readonly_stream()
        {
            var stream = _store.OpenReadOnly("id");
            Assert.IsType<ReadOnlyStream>(stream);
            Assert.False(stream.IsWritable);
        }

        [Fact]
        public void open_random_access_should_return_stream()
        {
            var stream = _store.OpenRandomAccess("random");
            Assert.IsType<Stream>(stream);
            Assert.True(stream.IsWritable);
        }
    }
}
