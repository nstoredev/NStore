using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;
using NStore.Snapshots;
using NStore.Streams;

namespace NStore.Aggregates
{
    public class Repository : IRepository
    {
        private readonly IAggregateFactory _factory;
        private readonly IStreamsFactory _streams;
        private readonly IDictionary<string, IStream> _openedStreams = new Dictionary<string, IStream>();
        private readonly IDictionary<string, IAggregate> _trackingAggregates = new Dictionary<string, IAggregate>();
        private readonly ISnapshotStore _snapshots;
        public bool PersistEmptyChangeset { get; set; } = false;

        public Repository(IAggregateFactory factory, IStreamsFactory streams)
            : this(factory, streams, null)
        {
        }

        public Repository(IAggregateFactory factory, IStreamsFactory streams, ISnapshotStore snapshots)
        {
            _factory = factory;
            _streams = streams;
            _snapshots = snapshots;
        }

        public Task<T> GetById<T>(string id) where T : IAggregate
        {
            return this.GetById<T>(id, CancellationToken.None);
        }

        public async Task<T> GetById<T>(
            string id,
            CancellationToken cancellationToken
        ) where T : IAggregate
        {
            if (_trackingAggregates.TryGetValue(id, out IAggregate aggregate))
            {
                return (T)aggregate;
            }

            aggregate = _factory.Create<T>();
            var persister = (IEventSourcedAggregate)aggregate;

            SnapshotInfo snapshot = null;

            if (_snapshots != null && aggregate is ISnaphottable snaphottable)

            {
                snapshot = await _snapshots.GetLastAsync(id, cancellationToken).ConfigureAwait(false);
                if (snapshot != null)
                {
                    //@@REVIEW: invalidate snapshot on false?
                    snaphottable.TryRestore(snapshot);
                }
            }

            if (!aggregate.IsInitialized)
            {
                aggregate.Init(id);
            }

            _trackingAggregates.Add(id, aggregate);

            var stream = OpenStream(id);

            int readCount = 0;
            var consumer = ConfigureConsumer(new LambdaSubscription(data =>
            {
                readCount++;
                persister.ApplyChanges((Changeset)data.Payload);
                return Task.FromResult(true);
            }), cancellationToken);

            // we use aggregate.Version because snapshot could be rejected
            // Starting point is inclusive, so almost one changeset should be loaded
            // aggregate will ignore because ApplyChanges is idempotent
            await stream.ReadAsync(consumer, aggregate.Version, long.MaxValue, cancellationToken)
                .ConfigureAwait(false);

            persister.Loaded();

            // no data from stream, we cannot validate the aggregate
            if (snapshot != null && readCount == 0)
            {
                throw new StaleSnapshotException(snapshot.SourceId, snapshot.SourceVersion);
            }

            return (T)aggregate;
        }

        protected virtual ISubscription ConfigureConsumer(ISubscription consumer, CancellationToken token)
        {
            return consumer;
        }

        public Task Save<T>(T aggregate, string operationId) where T : IAggregate
        {
            return this.Save<T>(aggregate, operationId, null, default(CancellationToken));
        }

        public Task Save<T>(T aggregate, string operationId, Action<IHeadersAccessor> headers) where T : IAggregate
        {
            return this.Save<T>(aggregate, operationId, headers, default(CancellationToken));
        }

        public async Task Save<T>(
            T aggregate,
            string operationId,
            Action<IHeadersAccessor> headers,
            CancellationToken cancellationToken
        ) where T : IAggregate
        {
            var persister = (IEventSourcedAggregate)aggregate;
            var changeSet = persister.GetChangeSet();
            if (changeSet.IsEmpty && !PersistEmptyChangeset)
                return;

            if (aggregate is IInvariantsChecker checker)
            {
                var check = checker.CheckInvariants();
                check.ThrowIfInvalid();
            }

            var stream = GetStream(aggregate);
            if (!stream.IsWritable)
            {
                throw new AggregateReadOnlyException();
            }

            headers?.Invoke(changeSet);

            await stream.AppendAsync(changeSet, operationId, cancellationToken).ConfigureAwait(false);
            persister.Persisted(changeSet);

            if (_snapshots != null && aggregate is ISnaphottable snaphottable)
            {
                //we need to await, it's responsibility of the snapshot provider to clone & store state (sync or async)
                await _snapshots.AddAsync(aggregate.Id, snaphottable.GetSnapshot(), cancellationToken).ConfigureAwait(false);
            }
        }

        protected virtual IStream OpenStream(string streamId)
        {
            if (_openedStreams.TryGetValue(streamId, out IStream stream))
            {
                return stream;
            }

            stream = _streams.OpenOptimisticConcurrency(streamId);
            _openedStreams.Add(streamId, stream);
            return stream;
        }


        private IStream GetStream(IAggregate aggregate)
        {
            if (!_trackingAggregates.Values.Contains(aggregate))
            {
                throw new RepositoryMismatchException();
            }

            try
            {
                return _openedStreams[aggregate.Id];
            }
            catch (KeyNotFoundException)
            {
                throw new RepositoryMismatchException();
            }
        }
    }
}