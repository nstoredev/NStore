using System;

namespace NStore.Domain
{
    public class InvariantCheckFailedException : Exception
    {
        public InvariantCheckFailedException(string message) : base(message)
        {
        }
    }
}