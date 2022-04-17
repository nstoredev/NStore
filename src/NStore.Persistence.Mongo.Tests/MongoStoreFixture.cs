using System;
using System.Reflection;
using System.Threading;
using MongoDB.Driver;
using NStore.Core.Persistence;
using NStore.Persistence.Mongo;
using Xunit;

#if MAP_DOMAIN
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Bson.Serialization.Serializers;
#endif

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace NStore.Persistence.Tests
{
    public partial class BaseStoreTest
    {
        protected string _mongoConnectionString;
        protected IMongoStore _mongoStore;
        private MongoStoreOptions _options;
        private const string TestSuitePrefix = "Mongo";

#if MAP_DOMAIN
        static BasePersistenceTest()
        {
            // enable support for dots in key names
            BsonClassMap.RegisterClassMap<Changeset>(map =>
            {
                map.AutoMap();
                map.MapProperty(x => x.Headers).SetSerializer(
                    new DictionaryInterfaceImplementerSerializer<
                        Dictionary<String, Object>
                    >(DictionaryRepresentation.ArrayOfArrays)
                );
            });
        }
#endif
        protected internal IStore Create(bool dropOnInit)
        {
            _mongoConnectionString = GetPartitionsConnectionString();
            _options = GetMongoStoreOptions();
            if (dropOnInit)
            {
                _options.DropOnInit = true;
            }
            _mongoStore = CreateStore(_options);

            _mongoStore.InitAsync(CancellationToken.None).Wait();

            return _mongoStore;
        }

        protected virtual internal MongoStoreOptions GetMongoStoreOptions()
        {
            return new MongoStoreOptions
            {
                PartitionsConnectionString = _mongoConnectionString,
                UseLocalSequence = true,
                PartitionsCollectionName = "partitions_" + GetType().Name + "_" + _testRunId,
                SequenceCollectionName = "seq_" + _testRunId
            };
        }

        private static string GetPartitionsConnectionString()
        {
            var mongo = Environment.GetEnvironmentVariable("NSTORE_MONGODB");
            if (string.IsNullOrWhiteSpace(mongo))
            {
                throw new TestMisconfiguredException("NSTORE_MONGODB environment variable not set");
            }

            return mongo;
        }

        protected IMongoCollection<TChunk> GetCollection<TChunk>()
        {
            var fieldInfo = _mongoStore.GetType()
                .GetField("_chunks", BindingFlags.NonPublic | BindingFlags.Instance);

            var collection = (IMongoCollection<TChunk>)fieldInfo.GetValue(_mongoStore);
            return collection;
        }

        protected virtual IMongoStore CreateStore(MongoStoreOptions options)
        {
            return new MongoStore(options);
        }

        protected void Clear(IStore store, bool drop)
        {
            // nothing to do
        }
    }
}