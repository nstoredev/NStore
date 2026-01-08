using System;
using System.Runtime.Serialization;

namespace NStore.Domain
{
    [Serializable]
    public class InvariantCheckFailedException : Exception
    {
        public InvariantCheckFailedException()
        {
        }

        public InvariantCheckFailedException(string message) : base(message)
        {
        }

        public InvariantCheckFailedException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}