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
        private static string Mongo;
        private static int _staticId = 1;
        private int _id;

        static BasePersistenceTest()
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("TEST_MONGODB");
            if (!string.IsNullOrEmpty(baseConnectionString))
            {
                var queryString = Environment.GetEnvironmentVariable("TEST_MONGODB_QUERYSTRING");
                Mongo = $"{baseConnectionString.TrimEnd('/')}/nstore{queryString}";
            }
            else
            {
                Mongo = "mongodb://localhost/nstore";
            }
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

            //           Console.WriteLine($"Setup {_id} {GetType().Name}");

            _mongoPersistence.InitAsync(CancellationToken.None).Wait();

            return _mongoPersistence;
        }

        private void Clear()
        {
            //          Console.WriteLine($"Cleanup {_id} {GetType().Name}");
            try
            {
                //    _mongoRawStore.Drop().Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR: {ex.Message}");
            }
        }
    }
}