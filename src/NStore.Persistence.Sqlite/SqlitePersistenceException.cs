using System;
using System.Runtime.Serialization;

namespace NStore.Persistence.Sqlite
{
    public class SqlitePersistenceException : Exception
    {
        public SqlitePersistenceException(string message) : base(message)
        {
        }
    }
}