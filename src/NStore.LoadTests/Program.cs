using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using App.Metrics;
using App.Metrics.Filtering;
using NStore.Core.InMemory;
using NStore.Core.Persistence;

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

    
        static async Task RunIoTSample()
        {
            var persistence = new InMemoryPersistence(new ReliableNetworkSimulator(4,6));
            var consumer = new IoTConsumer(workers:20, bufferSize:500, persistence: persistence);
            var producer = new IoTProducer(workers:3, bufferSize:1000, consumer: consumer);

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = 5
            };

            Parallel.ForEach(Enumerable.Range(1, 10000), options, async i =>
            {
                await producer.SimulateMessage(i).ConfigureAwait(false);
            });
            
            await producer.FlushAndShutDown().ConfigureAwait(false);
            await consumer.FlushAndShutDown().ConfigureAwait(false);
        }
    }
}