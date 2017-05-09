using System;
using System.Collections.Generic;
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
        private readonly IDictionary<IAggregate, IStream> _openedStreams = new Dictionary<IAggregate, IStream>();

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
            var stream = OpenStream(aggregate);
            var persister = (IAggregatePersister)aggregate;

            await stream.Read(new LambdaConsumer((l, payload) =>
                    {
                        var commit = (Commit)payload;

                        persister.AppendCommit(commit);
                        return ScanCallbackResult.Continue;
                    }),
                    0, version, cancellationToken)
                .ConfigureAwait(false);

            return aggregate;
        }

        public async Task Save<T>(
            T aggregate,
            string operationId,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate
        {
            var stream = GetStream(aggregate);
            var persister = (IAggregatePersister)aggregate;

            var commit = persister.BuildCommit();

            await stream.Append(commit, operationId, cancellationToken);
        }

        private IStream OpenStream(IAggregate aggregate)
        {
            var s = _streams.OpenOptimisticConcurrency(aggregate.Id);
            _openedStreams.Add(aggregate, s);
            return s;
        }

        private IStream GetStream(IAggregate aggregate)
        {
            return _openedStreams[aggregate];
        }
    }
}