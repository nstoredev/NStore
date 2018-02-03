using System;

namespace NStore.Domain
{
    public class AggregateFactory : IAggregateFactory
    {
        private readonly Func<Type, IAggregate> _factory = null;

        public AggregateFactory(Func<Type, IAggregate> factory)
        {
            _factory = factory;
        }

        public T Create<T>() where T : IAggregate
        {
            return (T)_factory(typeof(T));
        }

        public IAggregate Create(Type aggregateType)
        {
            return _factory(aggregateType);
        }
    }
}