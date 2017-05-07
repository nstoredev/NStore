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
            var stream = OpenStream(id);

            await stream.Read(0, Int32.MaxValue, (l, payload) =>
            {
                aggregate.Append(payload);
                return ScanCallbackResult.Continue;
            }, cancellationToken).ConfigureAwait(false);

            return aggregate;
        }

        public void Save<T>(T aggregate) where T : IAggregate
        {
        }

        private IStream OpenStream(string id)
        {
            return _streams.Open(id);
        }
    }
}