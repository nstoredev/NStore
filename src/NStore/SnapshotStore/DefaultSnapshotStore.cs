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

        public async Task<SnapshotInfo> Get(string id, int version, CancellationToken cancellationToken = default(CancellationToken))
        {
            SnapshotInfo snapshotInfo = null;

            await _store.ScanPartitionAsync(
                id,
                0,
                ScanDirection.Backward,
                new LambdaPartitionObserver((l, o) =>
                {
                    snapshotInfo = (SnapshotInfo)o;
                    return ScanCallbackResult.Stop;
                }),
                version,
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
                await _store.PersistAsync(aggregateId, snapshot.AggregateVersion, snapshot, cancellationToken: cancellationToken);
            }
            catch (DuplicateStreamIndexException)
            {
                // already stored
            }
        }
    }
}
