using System;

namespace NStore.Core.Logging
{
    public sealed class NStoreNullLogger : INStoreLogger
    {
        public static readonly INStoreLogger Instance = new NStoreNullLogger();
        private static readonly IDisposable FakeScopeInstance = new FakeScope();

        private sealed class FakeScope : IDisposable
        {
            public void Dispose()
            {
                // nothing do do here, it's a fake scope
            }
        }

        private NStoreNullLogger()
        {
        }

        public bool IsDebugEnabled => false;
        public void LogDebug(string message, params object[] args)
        {
            // this method was intentionally left empty.
        }

        public bool IsWarningEnabled => false;
        public void LogWarning(string message, params object[] args)
        {
            // this method was intentionally left empty.
        }

        public bool IsInformationEnabled => false;
        public void LogInformation(string message, params object[] args)
        {
            // this method was intentionally left empty.
        }

        public void LogError(string message, params object[] args)
        {
            // this method was intentionally left empty.
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return FakeScopeInstance;
        }
    }
}