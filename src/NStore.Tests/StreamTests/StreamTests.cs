using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NStore.Mongo;
using NStore.Raw.Contracts;
using NStore.Streams;
using Xunit;

namespace NStore.Tests.StreamTests
{
    public interface IStoreFactory
    {
        Task<IRawStore> Build();
    }

    public class StoreFactory : IStoreFactory
    {
        public const string MONGO = "mongodb://localhost/streams_tests";

        public async Task<IRawStore> Build()
        {
            var options = new MongoStoreOptions
            {
                StreamConnectionString = MONGO,
                UseLocalSequence = true
            };
            var raw = new MongoRawStore(options);
            await raw.DestroyStoreAsync();
            await raw.InitAsync();

            return raw;
        }
    }


    public class StreamTests
    {
        private readonly IRawStore _rawStore;
        private readonly IStreamStore _streams;

        public StreamTests()
        {
            var factory = new StoreFactory();
            _rawStore = factory.Build().Result;
            _streams = new StreamStore(_rawStore);
        }

        [Fact]
        public async void create_stream()
        {
            var stream = _streams.Open("stream_1");
            await stream.Append("payload");

            var acc = new Accumulator();
            await _rawStore.ScanAsync("stream_1", 0, ScanDirection.Forward, acc.Consume);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0]);
        }
    }
}