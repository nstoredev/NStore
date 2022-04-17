using System;

namespace NStore.Persistence.MsSql
{
    public class MsSqlStoreException : Exception
    {
        public MsSqlStoreException(string message) : base(message)
        {
        }
    }
}