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

        public TestLoggerFactory()
        {
            _logPoller = Environment.GetEnvironmentVariable("NSTORE-LOG-POLLER");
        }

        public ILogger CreateLogger(string categoryName)
        {
            if (_logPoller == null && categoryName == typeof(PollingClient).FullName)
            {
                return NullLogger.Instance;
            }

            return new ConsoleLogger(categoryName, (s, level) => true, true);
        }

        public void AddProvider(ILoggerProvider provider)
        {

        }

        public void Dispose()
        {

        }
    }
}
