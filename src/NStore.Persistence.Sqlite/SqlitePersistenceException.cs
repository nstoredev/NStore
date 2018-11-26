using System;
using System.Runtime.Serialization;

namespace NStore.Persistence.Sqlite
{
    public class SqlitePersistenceException : Exception
    {
        public SqlitePersistenceException(string message) : base(message)
        {
        }

        protected SqlitePersistenceException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}