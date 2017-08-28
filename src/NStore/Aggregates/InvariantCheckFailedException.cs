using System;

namespace NStore.Aggregates
{
    public class InvariantCheckFailedException : Exception
    {
        public InvariantCheckFailedException(string message) : base(message)
        {
        }
    }
}