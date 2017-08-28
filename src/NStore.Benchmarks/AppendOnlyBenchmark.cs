using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Attributes.Jobs;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Persistence.Mongo;
using NStore.Persistence;

namespace NStore.Benchmarks
{
    [SimpleJob(launchCount: 1, warmupCount: 1, targetCount: 1, invocationCount: 1, id: "append_job")]
    [Config("columns=Mean,StdError,StdDev,OperationPerSecond,Min,Max")]
    public class AppendOnlyBenchmark
    {
        private MongoPersistence _mongoStore;
        private InMemoryPersistence _inmemoryStore;
        private IEnumerable<int> _iterations;
        private const string Mongo = "mongodb://localhost/nstore";
        private static int Id = 0;

        private IList<MongoPersistence> _mongoPersistence;

        [Params(10000)]
        public int Writes { get; set; }

        //        [Params(8, 32, 128)]
        [Params(4, 8, 16, 32, 64)]
        public int Workers { get; set; }

        [Benchmark]
        public void load_memory_parallel()
        {
            _inmemoryStore = new InMemoryPersistence();
            paralell_worker(_inmemoryStore);
        }

        [Benchmark]
        public void load_memory_async()
        {
            _inmemoryStore = new InMemoryPersistence();
            async_worker(_inmemoryStore);
        }

        [Benchmark]
        public void load_memory_task()
        {
            _inmemoryStore = new InMemoryPersistence();
            task_worker(_inmemoryStore);
        }

        [Benchmark]
        public void load_mongo_async()
        {
            var options = BuildMongoConnectionOptions();

            _mongoStore = new MongoPersistence(options);
            _mongoStore.InitAsync(CancellationToken.None).Wait();

            async_worker(_mongoStore);
        }

        [Benchmark]
        public void load_mongo_parallel()
        {
            var options = BuildMongoConnectionOptions();
            _mongoStore = new MongoPersistence(options);
            _mongoStore.InitAsync(CancellationToken.None).Wait();

            paralell_worker(_mongoStore);
        }

        [Benchmark]
        public void load_mongo_task()
        {
            var options = BuildMongoConnectionOptions();
            _mongoStore = new MongoPersistence(options);
            _mongoStore.InitAsync(CancellationToken.None).Wait();

            task_worker(_mongoStore);
        }


        private static MongoPersistenceOptions BuildMongoConnectionOptions()
        {
            var id = Interlocked.Increment(ref Id);

            var options = new MongoPersistenceOptions
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

        [GlobalSetup]
        public void Setup()
        {
            _iterations = Enumerable.Range(0, Writes);
            _mongoPersistence = new List<MongoPersistence>();
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _inmemoryStore = null;

            Task.WaitAll(
                _mongoPersistence.Select(x => x.Drop(CancellationToken.None)).ToArray()
            );
        }

        private void paralell_worker(IPersistence store)
        {
            _iterations.ForEachAsync(Workers, i =>
                store.AppendAsync("Stream_1", i, new { data = "this is a test" })
            ).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        private void async_worker(IPersistence store)
        {
            var all = _iterations.Select(i =>
                store.AppendAsync("Stream_1", i, new { data = "this is a test" })
            );

            Task.WhenAll(all).GetAwaiter().GetResult();
        }

        private void task_worker(IPersistence store)
        {
            var all = _iterations.Select(i => Task.Run(async () =>
                await store.AppendAsync("Stream_1", i, new { data = "this is a test" })
            ));

            Task.WhenAll(all).GetAwaiter().GetResult();
        }
    }
}