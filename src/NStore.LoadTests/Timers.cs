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

        public static readonly TimerOptions ReadForward = new TimerOptions
        {
            Name = "ReadForward",
            MeasurementUnit = Unit.Requests,
            DurationUnit = TimeUnit.Milliseconds,
            RateUnit = TimeUnit.Milliseconds
        };
       
        public static readonly TimerOptions ReadBackward = new TimerOptions
        {
            Name = "ReadBackward",
            MeasurementUnit = Unit.Requests,
            DurationUnit = TimeUnit.Milliseconds,
            RateUnit = TimeUnit.Milliseconds
        };

        public static readonly TimerOptions ReadSingleBackward = new TimerOptions
        {
            Name = "ReadSingleBackward",
            MeasurementUnit = Unit.Requests,
            DurationUnit = TimeUnit.Milliseconds,
            RateUnit = TimeUnit.Milliseconds
        };

        public static readonly TimerOptions ReadAll = new TimerOptions
        {
            Name = "ReadAll",
            MeasurementUnit = Unit.Requests,
            DurationUnit = TimeUnit.Milliseconds,
            RateUnit = TimeUnit.Milliseconds
        };

        public static readonly TimerOptions ReadLastPosition = new TimerOptions
        {
            Name = "ReadLastPosition",
            MeasurementUnit = Unit.Requests,
            DurationUnit = TimeUnit.Milliseconds,
            RateUnit = TimeUnit.Milliseconds
        };     
        
        public static readonly TimerOptions Append = new TimerOptions
        {
            Name = "Append",
            MeasurementUnit = Unit.Requests,
            DurationUnit = TimeUnit.Milliseconds,
            RateUnit = TimeUnit.Milliseconds
        };
        
        public static readonly TimerOptions Delete = new TimerOptions
        {
            Name = "Delete",
            MeasurementUnit = Unit.Requests,
            DurationUnit = TimeUnit.Milliseconds,
            RateUnit = TimeUnit.Milliseconds
        };
        
        public static readonly TimerOptions ReadByOperationId = new TimerOptions
        {
            Name = "ReadByOperationId",
            MeasurementUnit = Unit.Requests,
            DurationUnit = TimeUnit.Milliseconds,
            RateUnit = TimeUnit.Milliseconds
        };
        
        public static readonly TimerOptions ReadAllByOperationId = new TimerOptions
        {
            Name = "ReadAllByOperationId",
            MeasurementUnit = Unit.Requests,
            DurationUnit = TimeUnit.Milliseconds,
            RateUnit = TimeUnit.Milliseconds
        };
    }
}