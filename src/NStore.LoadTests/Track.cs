using System;
using System.Threading;
using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Scheduling;

namespace NStore.LoadTests
{
    public static class Counters
    {
        public static readonly CounterOptions ReceivedMessages = new CounterOptions {Name = "Received Messages"};
    }

    public static class Track
    {
        private static IMetricsRoot _metrics;
        private static readonly CancellationTokenSource _cts = new CancellationTokenSource();
        public static void Init(IMetricsRoot metrics)
        {
            _metrics = metrics;
        }

        public static void Inc(CounterOptions counter)
        {
            _metrics.Measure.Counter.Increment(counter);
        }

        public static void StartReporter(TimeSpan delay)
        {
            Task.Factory.StartNew(
                async () =>
                {
//                    while (!_cts.Token.IsCancellationRequested)
                    {
                        await Task.Delay(delay, _cts.Token).ConfigureAwait(false);
                        await Task.WhenAll(_metrics.ReportRunner.RunAllAsync(_cts.Token)).ConfigureAwait(false);
                    }
                }, 
                TaskCreationOptions.LongRunning
            );
        }

        public static void Shutdown()
        {
            _cts.Cancel();
        }
    }
}