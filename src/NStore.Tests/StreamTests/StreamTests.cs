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
            var store = new MongoStore(options);
            await store.DestroyStoreAsync();
            await store.InitAsync();
            return store;
        }
    }


    public class StreamTests
    {
        private readonly IStoreFactory _factory;

        public StreamTests()
        {
            _factory = new StoreFactory();
        }

        [Fact]
        public async void create_stream()
        {
            var rawStore = await _factory.Build();
            IStream stream = new Stream("stream_1", rawStore);
            await stream.Append("payload");

            var acc = new Accumulator();
            await rawStore.ScanAsync("stream_1", 0, ScanDirection.Forward, acc.Consume);

            Assert.Equal(1, acc.Length);
            Assert.Equal("payload", acc[0]);
        }
    }
}