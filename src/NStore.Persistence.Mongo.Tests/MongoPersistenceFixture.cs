using System;
using System.Threading;
using NStore.Persistence.Mongo;
using NStore.Persistence;

// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private MongoPersistence _mongoPersistence;
        private MongoStoreOptions _options;
        private const string TestSuitePrefix = "Mongo";

        private IPersistence Create()
        {
            var mongo = Environment.GetEnvironmentVariable("NSTORE_MONGODB");
            if (string.IsNullOrWhiteSpace(mongo))
            {
                throw new TestMisconfiguredException("NSTORE_MONGODB environment variable not set");
            }

            _options = new MongoStoreOptions
            {
                PartitionsConnectionString = mongo,
                UseLocalSequence = true,
                PartitionsCollectionName = "partitions_" + GetType().Name + "_" + _testRunId,
                SequenceCollectionName = "seq_" + _testRunId,
                DropOnInit = true
            };
            _mongoPersistence = new MongoPersistence(_options);

            _mongoPersistence.InitAsync(CancellationToken.None).Wait();

            return _mongoPersistence;
        }

        private void Clear()
        {
            // nothing to do
        }
    }
}