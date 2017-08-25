using System;
using System.Runtime.Serialization;

namespace NStore.Persistence.Mongo
{
    [Serializable]
    public class MongoPersistenceException : Exception
    {
        public MongoPersistenceException()
        {
        }

        public MongoPersistenceException(string message) : base(message)
        {
        }

        public MongoPersistenceException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected MongoPersistenceException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}