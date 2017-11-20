using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using App.Metrics;
using App.Metrics.Filtering;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Persistence.Mongo;
using NStore.Tpl;

namespace NStore.LoadTests
{
    class Program
    {
        private static string _elasticUri = "http://127.0.0.1:9200";
        private static string _elasticIndex = "nstore";
        private static string _mongo = "mongodb://localhost/NStoreMetrics";

        static async Task Main(string[] args)
        {
            Console.WriteLine("ooooooooooooooooooooooooooooooooooooooooooooooo");
            Console.WriteLine(" S T A R T I N G");
            Console.WriteLine("ooooooooooooooooooooooooooooooooooooooooooooooo");

            var metrics = new MetricsBuilder()
                .Report.ToElasticsearch(_elasticUri, _elasticIndex)
                .Report.ToConsole()
                .Build();

            Track.Init(metrics);

            Track.StartReporter(TimeSpan.FromSeconds(1));
            await RunProducerConsumer().ConfigureAwait(false);

            await Track.FlushReporter().ConfigureAwait(false);

            Console.WriteLine("ooooooooooooooooooooooooooooooooooooooooooooooo");
            Console.WriteLine(" S T O P P E D - Press any key");
            Console.WriteLine("ooooooooooooooooooooooooooooooooooooooooooooooo");
            System.Console.ReadKey();
        }

        static IPersistence Connect()
        {
            return new InMemoryPersistence(new ReliableNetworkSimulator(10, 50));
        }

        static IPersistence MongoConnect()
        {
            var options = new MongoPersistenceOptions()
            {
                UseLocalSequence = true,
                PartitionsConnectionString = _mongo
            };
            var mongo= new MongoPersistence(options);
            mongo.InitAsync(CancellationToken.None).GetAwaiter().GetResult();
            return mongo;
        }

        static async Task RunProducerConsumer()
        {
//            var persistence = new MetricsPersistenceDecorator(MongoConnect());
            var persistence = Connect();
            
            var consumer = new Consumer(workers: 60, bufferSize: 20_000, persistence: persistence);
            var producer = new Producer(workers: 30, bufferSize: 20_000, consumer: consumer);

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount
            };

            Parallel.ForEach(Enumerable.Range(1, 10_000), options, async i =>
            {
                await producer.SimulateMessage(i).ConfigureAwait(false);
            });

            await producer.FlushAndShutDown().ConfigureAwait(false);
            await consumer.FlushAndShutDown().ConfigureAwait(false);
        }
    }
}