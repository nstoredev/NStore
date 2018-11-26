using System;
using System.Runtime.Serialization;

namespace NStore.Domain
{
    [Serializable]
    public class RepositoryMismatchException : Exception
    {
        public RepositoryMismatchException()
        {
        }

        public RepositoryMismatchException(string message) : base(message)
        {
        }

        public RepositoryMismatchException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected RepositoryMismatchException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}