using System;
using Microsoft.Extensions.Logging;
using NStore.Core.Logging;

namespace NStore.Tutorial.Support
{
    internal class ConsoleLoggerAdapter : INStoreLogger
    {
        private readonly ILogger _logger;

        public ConsoleLoggerAdapter(ILogger logger)
        {
            _logger = logger;
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

        public bool IsInformationEnabled => _logger.IsEnabled(LogLevel.Information);

        public void LogInformation(string message, params object[] args)
        {
            _logger.LogInformation(message, args);
        }

        public void LogError(string message, params object[] args)
        {
            _logger.LogError(message, args);
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return _logger.BeginScope(state);
        }
    }
}