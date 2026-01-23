using System.Threading;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Streams;
using Xunit;

namespace NStore.Core.Tests.Streams
{
    public class OptimisticConcurrenctyStreamTests
    {
        private readonly ProfileDecorator _persistence;
        private readonly OptimisticConcurrencyStream _stream;

        public OptimisticConcurrenctyStreamTests()
        {
            _persistence = new ProfileDecorator(new NullPersistence());
            _stream = new OptimisticConcurrencyStream("stream", _persistence);
        }

        [Fact]
        public async Task peek_should_read_last_chunk()
        {
            await _stream.PeekAsync().ConfigureAwait(false);
            Assert.Equal(1, _persistence.ReadSingleBackwardCounter.Calls);
        }
    }

    /// <summary>
    /// Tests for IsEmpty bug - see GitHub issue #128
    /// </summary>
    public class OptimisticConcurrencyStreamIsEmptyTests
    {
        private readonly InMemoryPersistence _persistence;

        public OptimisticConcurrencyStreamIsEmptyTests()
        {
            _persistence = new InMemoryPersistence();
        }

        [Fact]
        public async Task is_empty_should_return_true_for_empty_stream()
        {
            // Arrange
            var stream = new OptimisticConcurrencyStream("empty-stream", _persistence);

            // Act
            var isEmpty = await stream.IsEmpty(CancellationToken.None);

            // Assert - empty stream should return true
            Assert.True(isEmpty, "IsEmpty should return true for a stream with no data");
        }

        [Fact]
        public async Task is_empty_should_return_false_for_non_empty_stream()
        {
            // Arrange
            var stream = new OptimisticConcurrencyStream("non-empty-stream", _persistence);

            // First read to initialize version, then append data
            await stream.PeekAsync(CancellationToken.None);
            await stream.AppendAsync("some-payload", null, CancellationToken.None);

            // Act
            var isEmpty = await stream.IsEmpty(CancellationToken.None);

            // Assert - stream with data should return false
            Assert.False(isEmpty, "IsEmpty should return false for a stream with data");
        }
    }
}