using System;

namespace NStore.Core.Persistence
{
    [Serializable]
    public class PollingException : Exception
    {
        public PollingException() { }
        public PollingException(string message) : base(message) { }
        public PollingException(string message, Exception inner) : base(message, inner) { }
        protected PollingException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}