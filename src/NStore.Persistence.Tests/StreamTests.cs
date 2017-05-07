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
            await Store.ScanAsync("stream_1", 0, ScanDirection.Forward, acc.Record);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0]);
        }

        [Fact]
        public async void read_stream()
        {
            await Store.PersistAsync("stream_2", 1, "payload");

            var stream = _streams.Open("stream_2");
            var acc = new Tape();
            await stream.Read(0, acc.Record);

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
            await stream.Read(0, acc.Record);

            Assert.True(acc.IsEmpty);
        }
    }
}