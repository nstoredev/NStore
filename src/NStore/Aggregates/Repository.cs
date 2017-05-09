using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;
using NStore.Streams;

namespace NStore.Aggregates
{
    public sealed class Commit
    {
        public Object[] Events { get; private set; }
        public long Version { get; private set; }
        public bool IsEmpty => Events.Length == 0;

        private Commit()
        {

        }

        public Commit(long version, params object[] events)
        {
            this.Version = version;
            this.Events = events;
        }
    }

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
                        var commit = (Commit) payload;

                        persister.Append(commit);
                        return ScanCallbackResult.Continue;
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

            return aggregate;
        }

        public async Task Save<T>(
            T aggregate,
            string operationId,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate
        {
            var stream = OpenStream(aggregate.Id);
            var persister = (IAggregatePersister)aggregate;

            var commit = persister.BuildCommit();

            await stream.Append(commit, operationId);
        }

        private IStream OpenStream(string id)
        {
            return _streams.Open(id);
        }
    }
}