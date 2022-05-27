using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Options;
using NStore.Core.Logging;
using NStore.Core.Persistence;

namespace NStore.Persistence.Tests
{
    public class TestLoggerFactory : INStoreLoggerFactory
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

        public INStoreLogger CreateLogger(string categoryName)
        {
            var level = _level;

            if (level == "none")
                return NStoreNullLogger.Instance;

            Func<string, LogLevel, bool> filter = (s, l) => true;

            if (level == "info")
            {
                filter = (s, l) => l == LogLevel.Information;
            }

            if ((_logPoller == null || _logPoller == "0" || _logPoller == "none") &&
                categoryName == typeof(PollingClient).FullName)
            {
                return NStoreNullLogger.Instance;
            }

            var provider = new ConsoleLoggerProvider(new OptionsMonitor<ConsoleLoggerOptions>(new ConsoleLoggerOptions()));
            return new ConsoleLoggerWrapper(provider);
        }
    }

    public class OptionsMonitor<T> : IOptionsMonitor<T>
    {
        private readonly T options;

        public OptionsMonitor(T options)
        {
            this.options = options;
        }

        public T CurrentValue => options;

        public T Get(string name) => options;

        public IDisposable OnChange(Action<T, string> listener) => new NullDisposable();

        private class NullDisposable : IDisposable
        {
            public void Dispose() { }
        }
    }

    public class ConsoleLoggerWrapper : INStoreLogger
    {
        private ILogger _logger;

        public ConsoleLoggerWrapper(ConsoleLoggerProvider logger)
        {
            _logger = logger.CreateLogger("default-console");
        }

        public bool IsDebugEnabled => _logger.IsEnabled(LogLevel.Debug);

        public void LogDebug(string message, params object[] args)
        {
            _logger.LogDebug(message, args);
        }

        public bool IsWarningEnabled => _logger.IsEnabled(LogLevel.Warning);

        public void LogWarning(string message, params object[] args)
        {
            _logger.LogWarning(message, args);
        }

        public void LogError(string message, params object[] args)
        {
            _logger.LogError(message, args);

        }
        
        public bool IsInformationEnabled => _logger.IsEnabled(LogLevel.Information);

        public void LogInformation(string message, params object[] args)
        {
            _logger.LogInformation(message, args);

        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return _logger.BeginScope(state);
        }
    }
}