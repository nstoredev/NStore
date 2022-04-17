using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Core.Snapshots
{
    public class DefaultSnapshotStore : ISnapshotStore
    {
        private readonly IStore _store;

        public DefaultSnapshotStore(IStore store)
        {
            _store = store;
        }

        public async Task<SnapshotInfo> GetLastAsync(string snapshotPartitionId, CancellationToken cancellationToken)
        {
            var data = await _store.ReadSingleBackwardAsync(snapshotPartitionId, long.MaxValue, cancellationToken).ConfigureAwait(false);
            return (SnapshotInfo)data?.Payload;
        }

        public async Task<SnapshotInfo> GetAsync(string snapshotPartitionId, long version, CancellationToken cancellationToken)
        {
            var data = await _store.ReadSingleBackwardAsync(snapshotPartitionId, version, cancellationToken).ConfigureAwait(false);
            return (SnapshotInfo) data?.Payload;
        }

        public async Task AddAsync(string snapshotPartitionId, SnapshotInfo snapshot, CancellationToken cancellationToken)
        {
            if (snapshot == null || snapshot.IsEmpty)
                return;

            try
            {
                await _store.AppendAsync(snapshotPartitionId, snapshot.SourceVersion, snapshot, null, cancellationToken).ConfigureAwait(false);
            }
            catch (DuplicateStreamIndexException)
            {
                // already stored
            }
        }

        public Task DeleteAsync(string snapshotPartitionId, long fromVersionInclusive, long toVersionInclusive, CancellationToken cancellationToken)
        {
            return _store.DeleteAsync(snapshotPartitionId, fromVersionInclusive, toVersionInclusive, cancellationToken);
        }
    }
}
