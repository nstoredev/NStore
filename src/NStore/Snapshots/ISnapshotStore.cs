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

    public static class SnapshotStoreExtensions
    {
        public static Task<SnapshotInfo> GetAsync(
            this ISnapshotStore snapshots,
            string snapshotPartitionId,
            long version
        )
        {
            return snapshots.GetAsync(snapshotPartitionId, version, CancellationToken.None);
        }

        public static Task AddAsync(
            this ISnapshotStore snapshots,
            string snapshotPartitionId,
            SnapshotInfo snapshot
        )
        {
            return snapshots.AddAsync(snapshotPartitionId, snapshot, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this ISnapshotStore snapshots,
            string snapshotPartitionId
        )
        {
            return snapshots.DeleteAsync(snapshotPartitionId, 0, long.MaxValue, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this ISnapshotStore snapshots,
            string snapshotPartitionId,
            long fromVersionInclusive,
            long toVersionInclusive
        )
        {
            return snapshots.DeleteAsync(snapshotPartitionId, fromVersionInclusive, toVersionInclusive, CancellationToken.None);
        }
    }
}