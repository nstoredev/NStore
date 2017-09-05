using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Attributes.Jobs;
using BenchmarkDotNet.Engines;
using NStore.Core.Persistence;
using NStore.Persistence.Mongo;
using NStore.Tpl;

namespace NStore.Benchmarks
{
    [SimpleJob(launchCount: 3, warmupCount: 1, targetCount: 1, invocationCount: 3, id: "append_job")]
    [Config("columns=Mean,StdError,StdDev,OperationPerSecond,Min,Max")]
    public class BatchWriteBenchmark
    {
        private const string Mongo = "mongodb://localhost/NStore";
        private static int Id = 0;
        private IList<MongoPersistence> _mongoPersistence;
        private IEnumerable<int> _iterations;

        [Params(10_000)]
        public int Writes { get; set; }

        public int Workers { get; set; } = Environment.ProcessorCount * 100;

        [Benchmark]
        public void load_mongo_async()
        {
            var options = BuildMongoConnectionOptions();

            var store  = new MongoPersistence(options);
            _mongoPersistence.Add(store);
            store.InitAsync(CancellationToken.None).Wait();

            async_worker(store).GetAwaiter().GetResult();
        }

        [Benchmark]
        public void load_mongo_batcher()
        {
            var options = BuildMongoConnectionOptions();

            var store = new MongoPersistence(options);
            _mongoPersistence.Add(store);
            store.InitAsync(CancellationToken.None).Wait();

            var persistenceBatcher = new PersistenceBatcher(store);
            async_worker2(persistenceBatcher).GetAwaiter().GetResult();
            persistenceBatcher.Dispose();
        }

        private async Task async_worker(IPersistence store)
        {
            var publish = new ActionBlock<long>(async i =>
            {
                await store.AppendAsync("Stream_1", i, new { data = "this is a test" });
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount
            });

            foreach (var iteration in _iterations)
            {
                await publish.SendAsync(iteration);
            }

            publish.Complete();
            await publish.Completion;
        }
        
        private  Task async_worker2(IPersistence store)
        {
            var list = new List<Task>();
            
            foreach (var iteration in _iterations)
            {
                list.Add(store.AppendAsync("Stream_1", iteration, new { data = "this is a test" }));
            }

            return Task.WhenAll(list.ToArray());
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
//            Task.WaitAll(
//                _mongoPersistence.Select(x => x.Drop(CancellationToken.None)).ToArray()
//            );
        }
    }
}
