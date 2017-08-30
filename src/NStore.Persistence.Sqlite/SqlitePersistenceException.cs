using System;

namespace NStore.Persistence.Sqlite
{
    public class SqlitePersistenceException : Exception
    {
        public SqlitePersistenceException(string message) : base(message)
        {
        }
    }
}