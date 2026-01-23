using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Snapshots
{
    /// <summary>
    /// Abstraction for persisting and retrieving snapshots for a given snapshot partition.
    /// Implementations store SnapshotInfo instances keyed by <paramref name="snapshotPartitionId"/>.
    /// </summary>
    public interface ISnapshotStore
    {
        /// <summary>
        /// Retrieves the most recent snapshot for the given partition.
        /// </summary>
        /// <param name="snapshotPartitionId">Logical partition id used to group snapshots (e.g. aggregate id).</param>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>
        /// A <see cref="SnapshotInfo"/> representing the latest snapshot, or null if no snapshot exists for the partition.
        /// </returns>
        Task<SnapshotInfo> GetLastAsync(
            string snapshotPartitionId,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Retrieves the snapshot for the specified partition at the given version.
        /// </summary>
        /// <param name="snapshotPartitionId">Logical partition id used to group snapshots.</param>
        /// <param name="version">Snapshot version to retrieve.</param>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>
        /// A <see cref="SnapshotInfo"/> for the specified version, or null if no snapshot exists at that version.
        /// </returns>
        Task<SnapshotInfo> GetAsync(
            string snapshotPartitionId, 
            long version, 
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Persists a snapshot for the given partition.
        /// </summary>
        /// <param name="snapshotPartitionId">Logical partition id used to group snapshots.</param>
        /// <param name="snapshot">Snapshot metadata and payload to persist. Implementations typically use the snapshot's version to determine ordering.</param>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <remarks>
        /// Implementations may either insert or upsert. It is recommended to reject or ignore snapshots
        /// with versions older than the currently stored snapshot to avoid regressions.
        /// </remarks>
        Task AddAsync(
            string snapshotPartitionId, 
            SnapshotInfo snapshot, 
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Deletes snapshots for the given partition within the inclusive version range.
        /// </summary>
        /// <param name="snapshotPartitionId">Logical partition id used to group snapshots.</param>
        /// <param name="fromVersionInclusive">Start of deletion range (inclusive).</param>
        /// <param name="toVersionInclusive">End of deletion range (inclusive).</param>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <remarks>
        /// Implementations should ensure the range is applied atomically when possible.
        /// Passing a range that does not match any snapshot should succeed silently.
        /// </remarks>
        Task DeleteAsync(
            string snapshotPartitionId, 
            long fromVersionInclusive, 
            long toVersionInclusive, 
            CancellationToken cancellationToken
        );
    }
}