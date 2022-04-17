using System;
using System.Runtime.Serialization;

namespace NStore.Persistence.Sqlite
{
    public class SqliteStoreException : Exception
    {
        public SqliteStoreException(string message) : base(message)
        {
        }

        protected SqliteStoreException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}