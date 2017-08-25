using System.Threading;
using System.Threading.Tasks;

namespace NStore.Snapshots
{
    public class NullSnapshots : ISnapshotStore
    {
        public Task<SnapshotInfo> GetLastAsync(string snapshotPartitionId, CancellationToken cancellationToken)
        {
            return Task.FromResult((SnapshotInfo)null);

        }

        public Task<SnapshotInfo> GetAsync(string snapshotPartitionId, long version, CancellationToken cancellationToken)
        {
            return Task.FromResult((SnapshotInfo)null);
        }

        public Task AddAsync(string snapshotPartitionId, SnapshotInfo snapshot, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string snapshotPartitionId, long fromVersionInclusive, long toVersionInclusive, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}