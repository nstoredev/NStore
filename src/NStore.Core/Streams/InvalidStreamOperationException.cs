using System;
using System.Runtime.Serialization;

namespace NStore.Core.Streams
{
    [Serializable]
    public class InvalidStreamOperationException : Exception
    {
        public InvalidStreamOperationException()
        {
        }

        public InvalidStreamOperationException(string message) : base(message)
        {
        }

        public InvalidStreamOperationException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected InvalidStreamOperationException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}