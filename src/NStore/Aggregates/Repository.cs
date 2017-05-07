using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Aggregates
{
    public class Repository : IRepository
    {
        private readonly IAggregateFactory _factory;

        public Repository(IAggregateFactory factory)
        {
            _factory = factory;
        }

        public Task<T> GetById<T>(
            string id,
            int version = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate
        {
            var aggregate = _factory.Create<T>();


            return Task.FromResult(aggregate);
        }

        public void Save<T>(T aggregate) where T : IAggregate
        {
        }
    }
}