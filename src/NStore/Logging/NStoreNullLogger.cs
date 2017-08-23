using System;

namespace NStore.Logging
{
    public sealed class NStoreNullLogger : INStoreLogger
    {
        public static readonly INStoreLogger Instance = new NStoreNullLogger();
        private static readonly IDisposable EmptyScopeInstance = new EmptyScope();
        
        class EmptyScope : IDisposable
        {
            public void Dispose()
            {
            }
        }
        
        private NStoreNullLogger()
        {
        }

        public bool IsDebugEnabled => false;
        public void LogDebug(string message, params object[] args)
        {
        }

        public bool IsWarningEnabled => false;
        public void LogWarning(string message, params object[] args)
        {
        }

        public bool IsInformationEnabled => false;

        public void LogInformation(string message, params object[] args)
        {
        }

        public void LogError(string message, params object[] args)
        {
            
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return EmptyScopeInstance;
        }
    }
}