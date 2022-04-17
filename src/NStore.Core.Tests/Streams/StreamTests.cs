using System.Threading.Tasks;
using NStore.Core.Persistence;
using NStore.Core.Streams;
using Xunit;

namespace NStore.Core.Tests.Streams
{
    public class StreamTests
    {
        private readonly ProfileDecorator _store;
        private readonly Stream _stream;

        public StreamTests()
        {
            _store = new ProfileDecorator(new NullStore());
            _stream = new Stream("stream", _store);
        }

        [Fact]
        public async Task peek_should_read_last_chunk()
        {
            await _stream.PeekAsync().ConfigureAwait(false);
            Assert.Equal(1, _store.ReadSingleBackwardCounter.Calls);
        }
    }
}