using System.Threading;
using System.Threading.Tasks;

namespace NStore.Snapshots
{
    public interface ISnapshotStore
    {
        Task<SnapshotInfo> GetLastAsync(
            string snapshotPartitionId,
            CancellationToken cancellationToken
        );

        Task<SnapshotInfo> GetAsync(
            string snapshotPartitionId, 
            long version, 
            CancellationToken cancellationToken
        );

        Task AddAsync(
            string snapshotPartitionId, 
            SnapshotInfo snapshot, 
            CancellationToken cancellationToken
        );

        Task DeleteAsync(
            string snapshotPartitionId, 
            long fromVersionInclusive, 
            long toVersionInclusive, 
            CancellationToken cancellationToken
        );
    }
}