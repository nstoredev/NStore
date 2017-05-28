using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.SnapshotStore
{
    public class DefaultSnapshotStore : ISnapshotStore
    {
        private readonly IRawStore _store;

        public DefaultSnapshotStore(IRawStore store)
        {
            _store = store;
        }

        public async Task<SnapshotInfo> Get(string aggregateId, int version, CancellationToken cancellationToken = default(CancellationToken))
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

        public async Task Add(string aggregateId, SnapshotInfo snapshot, CancellationToken cancellationToken = default(CancellationToken))
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
            int fromVersionInclusive = 0, 
            int toVersionInclusive = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return _store.DeleteAsync(aggregateId, fromVersionInclusive, toVersionInclusive, cancellationToken);
        }
    }
}
