using System;
using System.Runtime.Serialization;

namespace NStore.Persistence.Mongo
{
    [Serializable]
    public class MongoStoreException : Exception
    {
        public MongoStoreException()
        {
        }

        public MongoStoreException(string message) : base(message)
        {
        }

        public MongoStoreException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected MongoStoreException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}