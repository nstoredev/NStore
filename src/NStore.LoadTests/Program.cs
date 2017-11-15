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
            var metrics = new MetricsBuilder()
                .Report.ToConsole()
                .Build();

            Track.Init(metrics);

            Track.StartReporter(TimeSpan.FromSeconds(1));
            await WriteOnStream().ConfigureAwait(false);
            await Track.FlushReporter();

            System.Console.ReadKey();
        }

        static async Task WriteOnStream()
        {
            var persistence = new InMemoryPersistence();
            var payload = new byte[4096];

            var worker = new ActionBlock<int>(async i =>
                {
                    Track.Inc(Counters.Iterations);
                    await Task.Delay(3);
                    await persistence.AppendAsync("test", i, payload, i.ToString())
                        .ConfigureAwait(false);
                },
                new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = 40
                }
            );

            for (int c = 1; c <= 100000; c++)
            {
                await worker.SendAsync(c).ConfigureAwait(false);
            }

            await worker.Completion.ConfigureAwait(false);
        }
    }
}