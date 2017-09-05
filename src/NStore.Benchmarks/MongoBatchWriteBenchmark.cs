using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Attributes.Jobs;
using NStore.Persistence.Mongo;
using NStore.Tpl;

namespace NStore.Benchmarks
{
    [SimpleJob(launchCount: 3, warmupCount: 1, targetCount: 3, invocationCount: 20, id: "batcher")]
    [MemoryDiagnoser]
    [Config("columns=Mean,StdError,StdDev,OperationPerSecond,Min,Max")]
    public class MongoBatchWriteBenchmark
    {
        private const string Mongo = "mongodb://localhost/NStoreBatch";
        private static int Id = 0;
        private IList<MongoPersistence> _mongoPersistence;
        private IEnumerable<int> _iterations;

        [Params(10_000)]
        public int Events { get; set; }

        [Params(32, 256, 512)]
        public int BatchSize { get; set; }

        [Params(5, 10, 20, 50)]
        public int FlushTimeout { get; set; }

        [Benchmark]
        public void batcher_decorator()
        {
            var options = BuildMongoConnectionOptions();

            var store = new MongoPersistence(options);
            _mongoPersistence.Add(store);
            store.InitAsync(CancellationToken.None).Wait();

            var persistenceBatcher = new PersistenceBatcher(store, BatchSize, FlushTimeout);
            TaskWorker.Run(persistenceBatcher, _iterations).GetAwaiter().GetResult();
            persistenceBatcher.Dispose();
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
            _iterations = Enumerable.Range(0, Events);
            _mongoPersistence = new List<MongoPersistence>();
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            Task.WaitAll(
                _mongoPersistence.Select(x => x.Drop(CancellationToken.None)).ToArray()
            );
        }
    }
}
