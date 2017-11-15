using System;
using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Scheduling;

namespace NStore.LoadTests
{
    public static class Counters
    {
        public static readonly CounterOptions Iterations = new CounterOptions {Name = "iterations"};
    }

    public static class Track
    {
        private static IMetricsRoot _metrics;
        private static AppMetricsTaskScheduler _scheduler;

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
            _scheduler = new AppMetricsTaskScheduler(
                delay,
                () => Task.WhenAll(_metrics.ReportRunner.RunAllAsync())
            );
            _scheduler.Start();
        }

        public static async Task FlushReporter()
        {
            _scheduler?.Dispose();
            await Task.WhenAll(_metrics.ReportRunner.RunAllAsync()).ConfigureAwait(false);
        }
    }
}