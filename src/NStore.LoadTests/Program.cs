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
        static async Task Main(string[] args)
        {
            Console.WriteLine("ooooooooooooooooooooooooooooooooooooooooooooooo");
            Console.WriteLine(" S T A R T I N G");
            Console.WriteLine("ooooooooooooooooooooooooooooooooooooooooooooooo");

            var metrics = new MetricsBuilder()
                .Report.ToElasticsearch("http://127.0.0.1:9200", "nstore")
                .Report.ToConsole()
                .Build();

            Track.Init(metrics);

            Track.StartReporter(TimeSpan.FromSeconds(1));
            await RunIoTSample().ConfigureAwait(false);

            await Track.FlushReporter().ConfigureAwait(false);

            Console.WriteLine("ooooooooooooooooooooooooooooooooooooooooooooooo");
            Console.WriteLine(" S T O P P I N G - Press any key");
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
                PartitionsConnectionString = "mongodb://localhost/NStoreMetrics"
            };
            var mongo= new MongoPersistence(options);
            mongo.InitAsync(CancellationToken.None).GetAwaiter().GetResult();
            return mongo;
//          return new PersistenceBatchAppendDecorator(mongo, 2000, 5);
        }

        static async Task RunIoTSample()
        {
            var persistence = new MetricsPersistenceDecorator(Connect());
            
            var consumer = new Ingestor(workers: 30, bufferSize: 20000, persistence: persistence);
            var producer = new IoTProducer(workers: 30, bufferSize: 20000, consumer: consumer);

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = 5
            };

            Parallel.ForEach(Enumerable.Range(1, 50_000), options, async i =>
            {
                await producer.SimulateMessage(i).ConfigureAwait(false);
            });

            await producer.FlushAndShutDown().ConfigureAwait(false);
            await consumer.FlushAndShutDown().ConfigureAwait(false);
        }
    }
}