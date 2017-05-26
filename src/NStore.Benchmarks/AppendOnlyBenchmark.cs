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
    [SimpleJob(launchCount: 1, warmupCount: 1, targetCount: 1, invocationCount: 1, id: "append_job")]
    [Config("columns=Mean,StdError,StdDev,OperationPerSecond,Min,Max")]
    public class AppendOnlyBenchmark
    {
        private MongoRawStore _mongoStore;
        private InMemoryRawStore _inmemoryStore;
        private IEnumerable<int> _iterations;
        private const string Mongo = "mongodb://localhost/nstore";
        private static int Id = 0;

        private IList<MongoRawStore> _mongoRawStores;

        [Params(10000)]
        public int Writes { get; set; }

        //        [Params(8, 32, 128)]
        [Params(4, 8, 16, 32, 64)]
        public int Workers { get; set; }

        [Benchmark]
        public void load_memory_parallel()
        {
            _inmemoryStore = new InMemoryRawStore();
            paralell_worker(_inmemoryStore);
        }

        [Benchmark]
        public void load_memory_async()
        {
            _inmemoryStore = new InMemoryRawStore();
            async_worker(_inmemoryStore);
        }

        [Benchmark]
        public void load_memory_task()
        {
            _inmemoryStore = new InMemoryRawStore();
            task_worker(_inmemoryStore);
        }

        [Benchmark]
        public void load_mongo_async()
        {
            var options = BuildMongoConnectionOptions();

            _mongoStore = new MongoRawStore(options);
            _mongoStore.InitAsync().Wait();

            async_worker(_mongoStore);
        }

        [Benchmark]
        public void load_mongo_parallel()
        {
            var options = BuildMongoConnectionOptions();
            _mongoStore = new MongoRawStore(options);
            _mongoStore.InitAsync().Wait();

            paralell_worker(_mongoStore);
        }

        [Benchmark]
        public void load_mongo_task()
        {
            var options = BuildMongoConnectionOptions();
            _mongoStore = new MongoRawStore(options);
            _mongoStore.InitAsync().Wait();

            task_worker(_mongoStore);
        }


        private static MongoStoreOptions BuildMongoConnectionOptions()
        {
            var id = Interlocked.Increment(ref Id);

            var options = new MongoStoreOptions
            {
                PartitionsConnectionString = Mongo,
                UseLocalSequence = true,
                PartitionsCollectionName = "partitions_" + id,
                SequenceCollectionName = "seq_" + id,
                DropOnInit = true,
                CustomizePartitionSettings = settings =>
                {
                    settings.MaxConnectionPoolSize = 20000;
                    settings.WaitQueueSize = 40000;
                }
            };
            return options;
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

        private void paralell_worker(IRawStore store)
        {
            _iterations.ForEachAsync(Workers, i =>
                store.PersistAsync("Stream_1", i, new { data = "this is a test" })
            ).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        private void async_worker(IRawStore store)
        {
            var all = _iterations.Select(i =>
                store.PersistAsync("Stream_1", i, new { data = "this is a test" })
            );

            Task.WhenAll(all).GetAwaiter().GetResult();
        }

        private void task_worker(IRawStore store)
        {
            var all = _iterations.Select(i => Task.Run(async () =>
                await store.PersistAsync("Stream_1", i, new { data = "this is a test" })
            ));

            Task.WhenAll(all).GetAwaiter().GetResult();
        }
    }
}