using System;
using System.Runtime.Serialization;

namespace NStore.Core.Streams
{
    [Serializable]
    public class StreamReadOnlyException : Exception
    {
        public StreamReadOnlyException()
        {
        }

        public StreamReadOnlyException(string message) : base(message)
        {
        }

        public StreamReadOnlyException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected StreamReadOnlyException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}