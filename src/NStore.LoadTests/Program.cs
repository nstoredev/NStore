using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using App.Metrics;
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
            Track.Shutdown();

            Console.WriteLine("ooooooooooooooooooooooooooooooooooooooooooooooo");
            Console.WriteLine(" S T O P P I N G");
            Console.WriteLine("ooooooooooooooooooooooooooooooooooooooooooooooo");
            System.Console.ReadLine();
        }

    
        static async Task RunIoTSample()
        {
            var persistence = new InMemoryPersistence(new UnreliableNetworkSimulator());
            var consumer = new IoTConsumer(workers:3, bufferSize:10_000, persistence: persistence);
            var producer = new IoTProducer(workers:5, bufferSize:10_000,consumer: consumer);

            await producer.FlushAndShutDown().ConfigureAwait(false);
            await consumer.FlushAndShutDown().ConfigureAwait(false);
        }
    }
}