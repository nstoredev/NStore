using System;
using System.Collections.Generic;
using System.Text;
using NStore.InMemory;
using NStore.Streams;
using Xunit;

namespace NStore.Tests.Persistence
{
    public class StreamStoreTests
    {
        private readonly IStreamsFactory _store = new StreamsFactory(new InMemoryPersistence());

        [Fact]
        public void open_should_return_stream()
        {
            var stream = _store.Open("id");
            Assert.IsType(typeof(Stream), stream);
            Assert.True(stream.IsWritable);
        }

        [Fact]
        public void open_optimistic_should_return_optimistic_concurrency_enabled_stream()
        {
            var stream = _store.OpenOptimisticConcurrency("id");
            Assert.IsType(typeof(OptimisticConcurrencyStream), stream);
            Assert.True(stream.IsWritable);
        }

        [Fact]
        public void open_readonly_should_return_readonly_stream()
        {
            var stream = _store.OpenReadOnly("id");
            Assert.IsType(typeof(ReadOnlyStream), stream);
            Assert.False(stream.IsWritable);
        }
    }
}
