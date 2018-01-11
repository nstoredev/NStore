using System.Threading.Tasks;
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
}