using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using NStore.Core.Streams;

namespace NStore.Domain
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
            : this(factory, streams, (ISnapshotStore)null)
        {
        }

        public Repository(IAggregateFactory factory, IStreamsFactory streams, ISnapshotStore snapshots)
        {
            _factory = factory;
            _streams = streams;
            _snapshots = snapshots;
        }

        public Task<IAggregate> GetByIdAsync(Type aggregateType, string id)
        {
            return GetByIdAsync(aggregateType, id, CancellationToken.None);
        }

        public Task<T> GetByIdAsync<T>(string id) where T : IAggregate
        {
            return this.GetByIdAsync<T>(id, CancellationToken.None);
        }

        public async Task<IAggregate> GetByIdAsync(Type aggregateType, string id, CancellationToken cancellationToken)
        {
            if (_trackingAggregates.TryGetValue(id, out IAggregate aggregate))
            {
                return aggregate;
            }

            aggregate = _factory.Create(aggregateType);
            var persister = (IEventSourcedAggregate)aggregate;

            SnapshotInfo snapshot = null;

            if (_snapshots != null && aggregate is ISnapshottable snapshottable)
            {
                snapshot = await _snapshots.GetLastAsync(id, cancellationToken).ConfigureAwait(false);
                if (snapshot != null)
                {
                    //@@REVIEW: invalidate snapshot on false?
                    snapshottable.TryRestore(snapshot);
                }
            }

            if (!aggregate.IsInitialized)
            {
                aggregate.Init(id);
            }

            _trackingAggregates.Add(id, aggregate);
            var stream = OpenStream(id);

            int readCount = 0;
            var subscription = new LambdaSubscription(data =>
            {
                readCount++;
                persister.ApplyChanges((Changeset)data.Payload);
                return Task.FromResult(true);
            });
            var consumer = ConfigureConsumer(subscription, cancellationToken);

            // we use aggregate.Version because snapshot could be rejected
            // Starting point is inclusive, so almost one changeset should be loaded
            // aggregate will ignore because ApplyChanges is idempotent
            await stream.ReadAsync(consumer, aggregate.Version, long.MaxValue, cancellationToken).ConfigureAwait(false);

            //Check lambda subscription for errors.
            if (subscription.Failed)
                throw new RepositoryReadException($"Error reading aggregate {id}", subscription.LastError);

            persister.Loaded();

            // no data from stream, we cannot validate the aggregate
            if (snapshot != null && readCount == 0)
            {
                throw new StaleSnapshotException(snapshot.SourceId, snapshot.SourceVersion);
            }

            return aggregate;
        }

        public async Task<T> GetByIdAsync<T>(
            string id, 
            CancellationToken cancellationToken
        ) where T : IAggregate
        {
            return (T) await GetByIdAsync(typeof(T), id, cancellationToken).ConfigureAwait(false);
        }

        protected virtual ISubscription ConfigureConsumer(ISubscription consumer, CancellationToken token)
        {
            return consumer;
        }

        public Task SaveAsync(IAggregate aggregate, string operationId) 
        {
            return this.SaveAsync(aggregate, operationId, null, default(CancellationToken));
        }

        public Task SaveAsync(IAggregate aggregate, string operationId, Action<IHeadersAccessor> headers) 
        {
            return this.SaveAsync(aggregate, operationId, headers, default(CancellationToken));
        }

        public async Task SaveAsync(
            IAggregate aggregate,
            string operationId,
            Action<IHeadersAccessor> headers,
            CancellationToken cancellationToken
        ) 
        {
            var persister = (IEventSourcedAggregate)aggregate;
            var changeSet = persister.GetChangeSet();
            if (changeSet.IsEmpty() && !PersistEmptyChangeset)
                return;

            var stream = GetStream(aggregate);
            if (!stream.IsWritable)
            {
                throw new AggregateReadOnlyException();
            }

            if (aggregate is IInvariantsChecker checker)
            {
                var check = checker.CheckInvariants();

                // checks for idempotency 
                if (check.IsInvalid && await stream.ContainsOperationAsync(operationId).ConfigureAwait(false))
                {
                    return;
                }

                check.ThrowIfInvalid();
            }

            headers?.Invoke(changeSet);

            var chunk = await stream.AppendAsync(changeSet, operationId, cancellationToken).ConfigureAwait(false);
            //remember to check if the chunk was really persisted or skipped because of operationId idempotency.
            if (chunk != null)
            {
                persister.Persisted(changeSet);

                if (_snapshots != null && aggregate is ISnapshottable snapshottable)
                {
                    //we need to await, it's responsibility of the snapshot provider to clone & store state (sync or async)
                    await _snapshots.AddAsync(aggregate.Id, snapshottable.GetSnapshot(), cancellationToken)
                        .ConfigureAwait(false);
                }
            }
            else
            {
                Clear();
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
                throw new RepositoryMismatchException($"Aggregate {aggregate.Id} was not loaded by this repository");
            }

            return _openedStreams[aggregate.Id];
        }

        public void Clear()
        {
            _trackingAggregates.Clear();
            _openedStreams.Clear();
        }
    }
}