using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Console;

namespace NStore.Persistence.Tests
{
    public class TestLoggerFactory : ILoggerFactory
    {
        private readonly string _logPoller;
        private readonly string _provider;
        private readonly string _level;
        public TestLoggerFactory(string provider)
        {
            _provider = provider;
            _level = Environment.GetEnvironmentVariable("NSTORE_LOG_LEVEL") ?? "none";
            _logPoller = Environment.GetEnvironmentVariable("NSTORE_LOG_POLLER");
        }

        public ILogger CreateLogger(string categoryName)
        {
            var level = _level;

            if (level == "none")
                return NullLogger.Instance;

            Func<string, LogLevel, bool> filter = (s, l) => true;

            if (level == "info")
            {
                filter = (s, l) => l == LogLevel.Information;
            }

            if ((_logPoller == null || _logPoller == "0" || _logPoller == "none") && categoryName == typeof(PollingClient).FullName)
            {
                return NullLogger.Instance;
            }

            return new ConsoleLogger(
                _provider +"::"+categoryName,
                filter, 
                true
            );
        }

        public void AddProvider(ILoggerProvider provider)
        {
            // nothing to do
        }

        public void Dispose()
        {
            // nothing to do
        }
    }
}
