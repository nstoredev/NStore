using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;
using NStore.Streams;

namespace NStore.Aggregates
{
    public class Repository : IRepository
    {
        private readonly IAggregateFactory _factory;
        private readonly IStreamStore _streams;

        public Repository(IAggregateFactory factory, IStreamStore streams)
        {
            _factory = factory;
            _streams = streams;
        }

        public async Task<T> GetById<T>(
            string id,
            int version = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate
        {
            var aggregate = _factory.Create<T>();
            aggregate.Init(id);
            var stream = OpenStream(id);
            var persister = (IAggregatePersister)aggregate;

            await stream.Read(
                    0,
                    version,
                    (l, payload) =>
                    {
                        persister.Append(l, (object[])payload);
                        return ScanCallbackResult.Continue;
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

            return aggregate;
        }

        public Task Save<T>(
            T aggregate,
            string operationId,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate
        {
            var stream = OpenStream(aggregate.Id);

            return Task.FromResult(0);
        }

        private IStream OpenStream(string id)
        {
            return _streams.Open(id);
        }
    }
}