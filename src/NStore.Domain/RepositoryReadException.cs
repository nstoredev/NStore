using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace NStore.Domain
{
    /// <summary>
    /// An exception that will be raised if <see cref="Repository"/> component 
    /// cannot read events to restore an aggregate.
    /// </summary>
    [Serializable]
    public class RepositoryReadException : Exception
    {
        public RepositoryReadException()
        {
        }

        public RepositoryReadException(string message) : base(message)
        {
        }

        public RepositoryReadException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected RepositoryReadException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
