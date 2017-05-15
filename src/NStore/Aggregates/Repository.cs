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
                return (T) _identityMap[mapid];
            }

            var aggregate = _factory.Create<T>();
            _identityMap.Add(mapid, aggregate);

            var snapshot = _snapshots.Get(id, version);
            aggregate.Init(id, snapshot.Version, snapshot.Data);

            var stream = OpenStream(aggregate, version != Int32.MaxValue);
            var persister = (IAggregatePersister) aggregate;

            var consumer = new LambdaPartitionObserver((l, payload) =>
            {
                var changes = (Changeset) payload;

                persister.ApplyChanges(changes);
                return ScanCallbackResult.Continue;
            });

            await stream.Read(consumer, snapshot.Version, version, cancellationToken)
                .ConfigureAwait(false);

            return aggregate;
        }

        public async Task Save<T>(
            T aggregate,
            string operationId,
            Action<IHeadersAccessor> headers = null,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate
        {
            var persister = (IAggregatePersister) aggregate;
            var changeSet = persister.GetChangeSet();
            if (changeSet.IsEmpty)
                return;

            var stream = GetStream(aggregate);
            //@@TODO refactor => !stream.IsWritable
            if (stream is ReadOnlyStream)
            {
                throw new AggregateReadOnlyException();
            }

            headers?.Invoke(changeSet);

            await stream.Append(changeSet, operationId, cancellationToken).ConfigureAwait(false);

            if (_snapshots.Updatable)
            {
                var snapshot = persister.GetSnapshot();
                await _snapshots.Add(aggregate.Id, snapshot).ConfigureAwait(false);
            }

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