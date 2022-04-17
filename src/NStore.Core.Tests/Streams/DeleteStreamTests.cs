using System.Threading;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Streams;
using Xunit;

namespace NStore.Core.Tests.Streams
{
    public class DeleteStreamTests
    {
        private readonly Stream _stream;

        public DeleteStreamTests()
        {
            var persistence = new ProfileDecorator(new InMemoryStore());
            _stream = new Stream("stream", persistence);
        }

        [Fact]
        public async Task should_delete_before_index()
        {
            await _stream.AppendAsync("first");
            var index = (await _stream.AppendAsync("second")).Index;
            await _stream.DeleteBeforeAsync(index, CancellationToken.None);

            var recorder = new Recorder();
            await _stream.ReadAsync(recorder);

            Assert.Equal(1, recorder.Length);
        }
    }
}