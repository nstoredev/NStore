using System.Diagnostics;
using Serilog;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;

namespace NStore.Tutorial.Support
{
    class CallerEnricher : ILogEventEnricher
    {
        public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
        {
            var skip = 1;
            while (true)
            {
                var stack = new StackFrame(skip);
                if (!stack.HasMethod())
                {
                    logEvent.AddPropertyIfAbsent(new LogEventProperty("Caller", new ScalarValue("<unknown method>")));
                    return;
                }

                var method = EnhancedStackTrace.GetMethodDisplayString(stack.GetMethod());
                var caller = method.DeclaringType.Name;
                if (!caller.StartsWith("Serilog") &&
                    !caller.StartsWith("Microsoft") &&
                    !caller.StartsWith("NStore.Tutorial.Support.ConsoleLoggerAdapter") &&
                    !caller.StartsWith("System.Runtime.CompilerServices.AsyncMethodBuilderCore") &&
                    !caller.StartsWith("NStore.Core.Persistence.PersistenceExtensions")
                )
                {
                    var dump = method.ToString();
                    logEvent.AddPropertyIfAbsent(new LogEventProperty("Caller", new ScalarValue(dump)));
                }

                skip++;
            }
        }
    }

    static class LoggerCallerEnrichmentConfiguration
    {
        public static LoggerConfiguration WithCaller(this LoggerEnrichmentConfiguration enrichmentConfiguration)
        {
            return enrichmentConfiguration.With<CallerEnricher>();
        }
    }
}