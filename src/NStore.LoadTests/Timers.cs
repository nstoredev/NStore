using App.Metrics;
using App.Metrics.Timer;

namespace NStore.LoadTests
{
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
}