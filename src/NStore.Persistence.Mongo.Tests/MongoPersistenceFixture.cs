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
        private static readonly string Mongo;
        private static int _staticId = 1;
        private int _id;
        private const string TestSuitePrefix = "Mongo";
        static BasePersistenceTest()
        {
            Mongo = Environment.GetEnvironmentVariable("NSTORE_MONGODB");
            if (string.IsNullOrWhiteSpace(Mongo))
            {
                throw new Exception("NSTORE_MONGODB environment variable not set");
            }
/*
            if (!string.IsNullOrEmpty(baseConnectionString))
            {
                var queryString = Environment.GetEnvironmentVariable("TEST_MONGODB_QUERYSTRING");
                Mongo = $"{baseConnectionString.TrimEnd('/')}/nstore{queryString}";
            }
            else
            {
                Mongo = "mongodb://localhost/nstorex";
            }
*/
        }

        private IPersistence Create()
        {
            _id = Interlocked.Increment(ref _staticId);

            _options = new MongoStoreOptions
            {
                PartitionsConnectionString = Mongo,
                UseLocalSequence = true,
                PartitionsCollectionName = "partitions_" + GetType().Name + "_" + _id,
                SequenceCollectionName = "seq_" + _id,
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