using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;
using NStore.SnapshotStore;
using NStore.Streams;

namespace NStore.Aggregates
{
    public class Repository : IRepository
    {
        private readonly IAggregateFactory _factory;
        private readonly IStreamStore _streams;
        private readonly IDictionary<IAggregate, IStream> _openedStreams = new Dictionary<IAggregate, IStream>();
        private readonly IDictionary<string, IAggregate> _identityMap = new Dictionary<string, IAggregate>();
        private readonly ISnapshotStore _snapshots;

        public Repository(IAggregateFactory factory, IStreamStore streams, ISnapshotStore snapshots = null)
        {
            _factory = factory;
            _streams = streams;
            _snapshots = snapshots ?? new NullSnapshots();
        }

        public async Task<T> GetById<T>(
            string id,
            int version = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate
        {
            string mapid = id + "@" + version;
            if (_identityMap.ContainsKey(mapid))
            {
                return (T)_identityMap[mapid];
            }

            var aggregate = _factory.Create<T>();
            var persister = (IAggregatePersister)aggregate;
            var snapshot = await _snapshots.Get(id, version, cancellationToken);

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
            var consumer = new LambdaPartitionObserver((changesetIndex, changesetPayload) =>
            {
                readCount++;
                persister.ApplyChanges((Changeset)changesetPayload);
                return ScanCallbackResult.Continue;
            });

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

            _identityMap.Add(mapid, aggregate);

            return aggregate;
        }

        public async Task Save<T>(
            T aggregate,
            string operationId,
            Action<IHeadersAccessor> headers = null,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate
        {
            var persister = (IAggregatePersister)aggregate;
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

            //@@TODO await or not?
            await _snapshots.Add(aggregate.Id, persister.GetSnapshot(), cancellationToken);

            persister.ChangesPersisted(changeSet);
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