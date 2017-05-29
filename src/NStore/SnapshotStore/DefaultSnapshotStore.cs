using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;

namespace NStore.SnapshotStore
{
    public class DefaultSnapshotStore : ISnapshotStore
    {
        private readonly IPersistence _store;

        public DefaultSnapshotStore(IPersistence store)
        {
            _store = store;
        }

        public async Task<SnapshotInfo> Get(
            string aggregateId,
            int version,
            CancellationToken cancellationToken)
        {
            SnapshotInfo snapshotInfo = null;

            await _store.ReadPartitionBackward(
                aggregateId,
                version,
                new LambdaPartitionConsumer((l, o) =>
                {
                    snapshotInfo = (SnapshotInfo)o;
                    return ScanAction.Stop;
                }),
                0,
                1,
                cancellationToken
            );

            return snapshotInfo;
        }

        public async Task Add(
            string aggregateId,
            SnapshotInfo snapshot,
            CancellationToken cancellationToken)
        {
            if (snapshot == null || snapshot.IsEmpty)
                return;

            try
            {
                await _store.PersistAsync(aggregateId, snapshot.AggregateVersion, snapshot, null, cancellationToken);
            }
            catch (DuplicateStreamIndexException)
            {
                // already stored
            }
        }

        public Task Remove(
            string aggregateId,
            int fromVersionInclusive,
            int toVersionInclusive,
            CancellationToken cancellationToken)
        {
            return _store.DeleteAsync(aggregateId, fromVersionInclusive, toVersionInclusive, cancellationToken);
        }
    }
}
