using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Attributes.Jobs;
using Newtonsoft.Json;
using NStore.Core.Logging;
using NStore.Persistence.Mongo;
using NStore.Persistence.Sqlite;
using NStore.Tpl;

namespace NStore.Benchmarks
{
    public class SqliteSerializer : ISqlitePayloadSerializer
    {
        private readonly JsonSerializerSettings _settings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.All
        };

        public string Serialize(object payload)
        {
            return JsonConvert.SerializeObject(payload,_settings);
        }

        public object Deserialize(string serialized)
        {
            return JsonConvert.DeserializeObject(serialized, _settings);
        }
    }

    [SimpleJob(launchCount: 3, warmupCount: 1, targetCount: 3, invocationCount: 3, id: "sqlite")]
    [MemoryDiagnoser]
    [Config("columns=Mean,StdError,StdDev,OperationPerSecond,Min,Max")]
    public class SqliteBatchWriteBenchmark
    {
        private static int Id = 0;
        private IList<SqlitePersistence> _persistence;
        private IEnumerable<int> _iterations;

        [Params(1_000)]
        public int Events { get; set; }

        [Params(32, 256, 512)]
        public int BatchSize { get; set; }

        [Params(10, 100, 200)]
        public int FlushTimeout { get; set; }

        [Benchmark]
        public void batcher_decorator()
        {
            var options = BuildOptions();

            var store = new SqlitePersistence(options);
            _persistence.Add(store);
            store.InitAsync(CancellationToken.None).Wait();

            var persistenceBatcher = new PersistenceBatcher(store, BatchSize, FlushTimeout);
            TaskWorker.Run(persistenceBatcher, _iterations).GetAwaiter().GetResult();
            persistenceBatcher.Dispose();
        }

        private static SqlitePersistenceOptions BuildOptions()
        {
            var id = Interlocked.Increment(ref Id);

            var pathToDb = Path.Combine(Path.GetDirectoryName(Assembly.GetCallingAssembly().Location), $"test_batch_{id}.db");
            if(File.Exists(pathToDb))
                File.Delete(pathToDb);

            var options = new SqlitePersistenceOptions(NStoreNullLoggerFactory.Instance)
            {
                ConnectionString = $"Data Source={pathToDb}",
                Serializer = new SqliteSerializer()
            };
            return options;
        }

        [GlobalSetup]
        public void Setup()
        {
            _iterations = Enumerable.Range(0, Events);
            _persistence = new List<SqlitePersistence>();
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            //Task.WaitAll(
            //    _persistence.Select(x => x.Drop(CancellationToken.None)).ToArray()
            //);
        }
    }
}
