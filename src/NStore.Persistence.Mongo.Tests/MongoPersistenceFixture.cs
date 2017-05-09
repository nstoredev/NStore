using System;
using System.Threading;
using NStore.Persistence.Mongo;
using NStore.Raw;
// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private MongoRawStore _mongoRawStore;
        private MongoStoreOptions _options;
        private const string Mongo = "mongodb://localhost/nstore";
        private static int StaticId = 1;
        private int _id;

        private IRawStore Create()
        {
            _id = Interlocked.Increment(ref StaticId);

            _options = new MongoStoreOptions
            {
                PartitionsConnectionString = Mongo,
                UseLocalSequence = true,
                PartitionsCollectionName = "partitions_" + _id,
                SequenceCollectionName = "seq_" + _id,
                DropOnInit = true
            };
            _mongoRawStore = new MongoRawStore(_options);

            Console.WriteLine($"Setup {_id} {GetType().Name}");

            _mongoRawStore.InitAsync().Wait();

            return _mongoRawStore;
        }

        private void Clear()
        {
            Console.WriteLine($"Cleanup {_id} {GetType().Name}");
            try
            {
                _mongoRawStore.Drop().Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR: {ex.Message}");
            }
        }
    }
}