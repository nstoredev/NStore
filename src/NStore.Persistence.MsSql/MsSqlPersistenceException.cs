using System;

namespace NStore.Persistence.MsSql
{
    public class MsSqlPersistenceException : Exception
    {
        public MsSqlPersistenceException(string message) : base(message)
        {
        }
    }
}