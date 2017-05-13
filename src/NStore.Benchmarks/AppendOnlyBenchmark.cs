using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Attributes.Jobs;
using NStore.InMemory;
using NStore.Persistence.Mongo;
using NStore.Raw;

namespace NStore.Benchmarks
{
    [SimpleJob(launchCount: 1, warmupCount: 3, targetCount: 5, invocationCount:5, id: "append_job")]
    [Config("columns=Mean,StdError,StdDev,OperationPerSecond,Min,Max")]
    public class AppendOnlyBenchmark
    {
        private MongoRawStore _mongoStore;
        private InMemoryRawStore _inmemoryStore;
        private IEnumerable<int> _iterations;
        private const string Mongo = "mongodb://localhost/nstore";
        private static int Id = 0;

        private IList<MongoRawStore> _mongoRawStores;

        [Params(0, 64, 512, 1024)]
        public int Writes { get; set; }

        [Params(1, 8, 16, 32)]
        public int Workers { get; set; }


        [Benchmark]
        public void load_memory()
        {
            _inmemoryStore = new InMemoryRawStore();
            Worker(_inmemoryStore);
        }

        [Benchmark]
        public void load_mongo()
        {
            var id = Interlocked.Increment(ref Id);

            var options = new MongoStoreOptions
            {
                PartitionsConnectionString = Mongo,
                UseLocalSequence = true,
                PartitionsCollectionName = "partitions_" + id,
                SequenceCollectionName = "seq_" + id,
                DropOnInit = true
            };
            _mongoStore = new MongoRawStore(options);
            _mongoStore.InitAsync().Wait();

            Worker(_mongoStore);
        }

        [Setup]
        public void Setup()
        {
            _iterations = Enumerable.Range(0, Writes);
            _mongoRawStores = new List<MongoRawStore>();
        }

        [Cleanup]
        public void Cleanup()
        {
            _inmemoryStore = null;

            Task.WaitAll(
                _mongoRawStores.Select(x => x.Drop()).ToArray()
            );
        }

        private void Worker(IRawStore store)
        {
            _iterations.ForEachAsync(Workers, i =>
                store.PersistAsync("Stream_1", i, new { data = "this is a test" })
            ).ConfigureAwait(false).GetAwaiter().GetResult();
        }
    }
}