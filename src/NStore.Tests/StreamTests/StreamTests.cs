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
            IStream stream = new Stream("stream_1",await _factory.Build());
            await stream.Append("payload");
            //@@TODO check
        }
    }
}