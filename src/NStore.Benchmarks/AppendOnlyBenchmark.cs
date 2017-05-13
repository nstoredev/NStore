using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Attributes.Jobs;
using NStore.InMemory;
using NStore.Persistence.Mongo;
using NStore.Raw;

namespace NStore.Benchmarks
{
    [SimpleJob(launchCount: 1, warmupCount: 3, targetCount: 5, invocationCount:5, id: "append_job")]
    public class AppendOnlyBenchmark
    {
		private const string Mongo = "mongodb://localhost/nstore";

		[Benchmark]
        public void load_memory()
        {
            var store = new InMemoryRawStore();
            Worker(store, 4, 1000);
        }

		[Benchmark]
		public void load_mongo()
		{
			var _id = "bench";

			var _options = new MongoStoreOptions
			{
				PartitionsConnectionString = Mongo,
				UseLocalSequence = true,
				PartitionsCollectionName = "partitions_" + _id,
				SequenceCollectionName = "seq_" + _id,
				DropOnInit = true
			};
			var _mongoRawStore = new MongoRawStore(_options);

			_mongoRawStore.InitAsync().Wait();
            try
            {
				Worker(_mongoRawStore, 4, 1000);
			}
            finally
            {
				_mongoRawStore.Drop().Wait();
            }
		}

        private void Worker(IRawStore store, int dop, int number)
        {
            int max = number;
            var range = Enumerable.Range(0, max);

            range.ForEachAsync(dop, i =>
                store.PersistAsync("Stream_1", i, new {data = "this is a test"})
            ).ConfigureAwait(false).GetAwaiter().GetResult();
        }
    }
}