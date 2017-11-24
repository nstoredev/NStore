using System;

namespace NStore.Domain
{
    public class AggregateAlreadyInitializedException : Exception
    {
        public Type AggregateType { get; }
        public string AggregateId { get; }

        public AggregateAlreadyInitializedException(Type aggregateType, string aggregateId)
            :base($"Aggregate {aggregateId} of type {aggregateType.Name} has already been initialized.")
        {
            AggregateType = aggregateType;
            AggregateId = aggregateId;
        }
    }
}