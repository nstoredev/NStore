using System;

namespace NStore.Core.Persistence
{
    public class PollingException : Exception
    {
        public PollingException(string message) : base(message)
        {
        }
    }
}