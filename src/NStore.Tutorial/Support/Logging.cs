using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;
using NStore.Core.Logging;
using NStore.Core.Persistence;

namespace NStore.Tutorial.Support
{
    internal class LoggerFactoryAdapter : INStoreLoggerFactory
    {
        private readonly ILoggerFactory _loggerFactory;

        public LoggerFactoryAdapter(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
        }

        public INStoreLogger CreateLogger(string categoryName)
        {
            if (categoryName == typeof(PollingClient).FullName)
                return NStoreNullLogger.Instance;

            return new ConsoleLoggerAdapter(
                _loggerFactory.CreateLogger(categoryName)
            );
        }
    }
}
