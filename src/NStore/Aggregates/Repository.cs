using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;
using NStore.SnapshotStore;
using NStore.Streams;

namespace NStore.Aggregates
{
    public class Repository : IRepository
    {
        private readonly IAggregateFactory _factory;
        private readonly IStreamsFactory _streams;
        private readonly IDictionary<IAggregate, IStream> _openedStreams = new Dictionary<IAggregate, IStream>();
        private readonly ISnapshotStore _snapshots;

        public Repository(IAggregateFactory factory, IStreamsFactory streams)
            : this(factory, streams, null)
        {
        }

        public Repository(IAggregateFactory factory, IStreamsFactory streams, ISnapshotStore snapshots)
        {
            _factory = factory;
            _streams = streams;
            _snapshots = snapshots ?? new NullSnapshots();
        }

        public Task<T> GetById<T>(string id) where T : IAggregate
        {
            return this.GetById<T>(id, int.MaxValue);
        }

        public Task<T> GetById<T>(string id, CancellationToken cancellationToken) where T : IAggregate
        {
            return this.GetById<T>(id, int.MaxValue, cancellationToken);
        }

        public async Task<T> GetById<T>(
            string id,
            int version,
            CancellationToken cancellationToken
        ) where T : IAggregate
        {
            var aggregate = _factory.Create<T>();
            var persister = (IEventSourcedAggregate)aggregate;
            var snapshot = await _snapshots.Get(id, version, cancellationToken).ConfigureAwait(false);

            if (snapshot != null)
            {
                //@@REVIEW: invalidate snapshot on false?
                persister.TryRestore(snapshot);
            }

            if (!aggregate.IsInitialized)
            {
                aggregate.Init(id);
            }

            var stream = OpenStream(aggregate, version != Int32.MaxValue);

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
            await stream.Read(consumer, aggregate.Version, version, cancellationToken)
                .ConfigureAwait(false);

            // no data from stream, we cannot validate the aggregate
            if (snapshot != null && readCount == 0)
            {
                throw new StaleSnapshotException(snapshot.AggregateId, snapshot.AggregateVersion);
            }

            return aggregate;
        }

        protected virtual ISubscription ConfigureConsumer(ISubscription consumer, CancellationToken token)
        {
            return consumer;
        }

        public Task<T> GetById<T>(string id, int version) where T : IAggregate
        {
            return this.GetById<T>(id, version, default(CancellationToken));
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
            if (changeSet.IsEmpty)
                return;

            var stream = GetStream(aggregate);
            if (!stream.IsWritable)
            {
                throw new AggregateReadOnlyException();
            }

            headers?.Invoke(changeSet);

            await stream.Append(changeSet, operationId, cancellationToken).ConfigureAwait(false);
            persister.ChangesPersisted(changeSet);

            //we need to await, it's responsibility of the snapshot provider to clone & store state (sync or async)
            await _snapshots.Add(aggregate.Id, persister.GetSnapshot(), cancellationToken).ConfigureAwait(false);
        }

        private IStream OpenStream(IAggregate aggregate, bool isPartialLoad)
        {
            var stream = isPartialLoad
                ? _streams.OpenReadOnly(aggregate.Id)
                : _streams.OpenOptimisticConcurrency(aggregate.Id);

            _openedStreams.Add(aggregate, stream);
            return stream;
        }

        private IStream GetStream(IAggregate aggregate)
        {
            try
            {
                return _openedStreams[aggregate];
            }
            catch (KeyNotFoundException)
            {
                throw new RepositoryMismatchException();
            }
        }
    }
}