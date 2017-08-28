using System;

namespace NStore.Persistence
{
    public class PollingException : Exception
    {
        public PollingException(string message) : base(message)
        {
        }
    }
}