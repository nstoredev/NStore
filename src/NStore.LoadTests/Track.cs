using System;
using System.Threading;
using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Reporting.Console;
using App.Metrics.Scheduling;
using App.Metrics.Timer;

namespace NStore.LoadTests
{
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
        
        public static void Dec(CounterOptions counter)
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

        public static async Task Profile(TimerOptions timer, Func<Task> task)
        {
            using (_metrics.Measure.Timer.Time(timer))
            {
                await task().ConfigureAwait(false);
            }
        }

        public static async Task<T> Profile<T>(TimerOptions timer, Func<Task<T>> task)
        {
            using (_metrics.Measure.Timer.Time(timer))
            {
                return await task().ConfigureAwait(false);
            }
        }
    }
}