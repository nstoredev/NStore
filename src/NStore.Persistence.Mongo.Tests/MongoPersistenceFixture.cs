using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using MongoDB.Driver;
using NStore.Core.Persistence;
using NStore.Persistence.Mongo;
using NStore.Persistence;
#if MAP_DOMAIN
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Bson.Serialization.Serializers;
#endif

// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        protected IMongoPersistence _mongoPersistence;
        private MongoPersistenceOptions _options;
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
        private IPersistence Create()
        {
            var mongo = Environment.GetEnvironmentVariable("NSTORE_MONGODB");
            if (string.IsNullOrWhiteSpace(mongo))
            {
                throw new TestMisconfiguredException("NSTORE_MONGODB environment variable not set");
            }

            _options = new MongoPersistenceOptions
            {
                PartitionsConnectionString = mongo,
                UseLocalSequence = true,
                PartitionsCollectionName = "partitions_" + GetType().Name + "_" + _testRunId,
                SequenceCollectionName = "seq_" + _testRunId,
                DropOnInit = true
            };
            _mongoPersistence = CreatePersistence(_options);

            _mongoPersistence.InitAsync(CancellationToken.None).Wait();

            return _mongoPersistence;
        }
        protected IMongoCollection<TChunk> GetCollection<TChunk>()
        {
            var fieldInfo = _mongoPersistence.GetType()
                .GetField("_chunks", BindingFlags.NonPublic | BindingFlags.Instance);

            var collection = (IMongoCollection<TChunk>)fieldInfo.GetValue(_mongoPersistence);
            return collection;
        }

        protected virtual IMongoPersistence CreatePersistence(MongoPersistenceOptions options)
        {
            return new MongoPersistence(options);
        }

        private void Clear()
        {
            // nothing to do
        }
    }
}