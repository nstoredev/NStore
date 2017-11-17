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
    public static class Counters
    {
        public static readonly CounterOptions ReceivedMessages = new CounterOptions {Name = "Received Messages"};
        public static readonly CounterOptions SentMessages = new CounterOptions {Name = "Sent Messages"};
        public static readonly CounterOptions SimulatedMessages = new CounterOptions {Name = "Simulated Messages"};
    }

    public static class Timers
    {
        public static readonly TimerOptions RequestTimer = new TimerOptions
        {
            Name = "Request Timer",
            MeasurementUnit = Unit.Requests,
            DurationUnit = TimeUnit.Milliseconds,
            RateUnit = TimeUnit.Milliseconds
        };
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
    }
}