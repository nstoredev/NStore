using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Snapshots
{
    public static class SnapshotStoreExtensions
    {
        public static Task<SnapshotInfo> GetLastAsync(
            this ISnapshotStore snapshots, 
            string snapshotPartitionId
        )
        {
            return snapshots.GetLastAsync(snapshotPartitionId, CancellationToken.None);
        }

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

        /// <summary>
        /// Retrieves the most recent snapshots for multiple partitions without requiring a cancellation token.
        /// </summary>
        /// <param name="batchStore">The snapshot batch store instance.</param>
        /// <param name="snapshotPartitionIds">Collection of partition IDs to retrieve snapshots for.</param>
        /// <returns>
        /// A dictionary mapping partition IDs to their corresponding <see cref="SnapshotInfo"/>.
        /// Only partitions that have snapshots are included in the result.
        /// </returns>
        /// <remarks>
        /// This is a convenience overload that uses <see cref="CancellationToken.None"/>.
        /// For long-running operations or when cancellation support is needed, use the overload
        /// that accepts a <see cref="CancellationToken"/>.
        /// </remarks>
        public static Task<IReadOnlyDictionary<string, SnapshotInfo>> GetManyAsync(
            this ISnapshotBatchStore batchStore,
            IEnumerable<string> snapshotPartitionIds
        )
        {
            return batchStore.GetManyAsync(snapshotPartitionIds, CancellationToken.None);
        }

        /// <summary>
        /// Stores multiple snapshots without requiring a cancellation token.
        /// </summary>
        /// <param name="batchStore">The snapshot batch store instance.</param>
        /// <param name="snapshots">Dictionary mapping partition IDs to their snapshot information.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        /// This is a convenience overload that uses <see cref="CancellationToken.None"/>.
        /// Uses best-effort semantics - individual snapshot save failures are silently ignored.
        /// </remarks>
        public static Task AddManyAsync(
            this ISnapshotBatchStore batchStore,
            IReadOnlyDictionary<string, SnapshotInfo> snapshots
        )
        {
            return batchStore.AddManyAsync(snapshots, CancellationToken.None);
        }
    }
}