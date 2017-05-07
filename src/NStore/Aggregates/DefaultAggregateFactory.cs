using System;

namespace NStore.Aggregates
{
    public class DefaultAggregateFactory : IAggregateFactory
    {
        public T Create<T>() where T : IAggregate
        {
            var aggregate = Activator.CreateInstance<T>();

            return aggregate;
        }
    }
}