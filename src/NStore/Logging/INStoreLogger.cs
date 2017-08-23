using System;

namespace NStore.Logging
{
    public interface INStoreLogger
    {
        bool IsDebugEnabled { get; }
        void LogDebug(string message, params object[] args);
        
        bool IsWarningEnabled { get; }
        void LogWarning(string message, params object[] args);

        bool IsInformationEnabled { get; }
        void LogInformation(string message, params object[] args);

        void LogError(string message, params object[] args);


        IDisposable BeginScope<TState>(TState state);
        
    }
}