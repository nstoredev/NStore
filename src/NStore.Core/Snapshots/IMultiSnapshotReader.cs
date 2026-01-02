using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Snapshots
{
    /// <summary>
    /// Provides batch read operations for retrieving snapshots from multiple partitions.
    /// This interface enables efficient bulk retrieval when you need to load snapshots
    /// for many aggregates or entities in a single operation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This interface is designed to complement <see cref="ISnapshotStore"/> by providing
    /// batch read capabilities. Implementations may optimize batch reads at the storage level
    /// (e.g., using database batch queries) or execute individual reads in parallel.
    /// </para>
    /// <para>
    /// Typical use cases include:
    /// - Loading snapshots for multiple aggregates during query processing
    /// - Bulk snapshot retrieval for reporting or analysis
    /// - Preloading snapshots for a set of related entities
    /// </para>
    /// </remarks>
    public interface IMultiSnapshotReader
    {
        /// <summary>
        /// Retrieves the most recent snapshots for multiple partitions in a single operation.
        /// </summary>
        /// <param name="snapshotPartitionIds">Collection of partition IDs to retrieve snapshots for.</param>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>
        /// A dictionary mapping partition IDs to their corresponding <see cref="SnapshotInfo"/>.
        /// Only partitions that have snapshots are included in the result. If a partition has no snapshot,
        /// it will not appear in the returned dictionary.
        /// </returns>
        /// <remarks>
        /// <para>
        /// <strong>Return Value Semantics:</strong> The returned dictionary only contains entries for
        /// partitions where a snapshot exists. If you request 10 partition IDs but only 7 have snapshots,
        /// the dictionary will contain 7 entries. Check for the presence of a key to determine if a
        /// partition has a snapshot.
        /// </para>
        /// <para>
        /// <strong>Performance:</strong> Implementations may execute reads in parallel or use batch
        /// queries to optimize performance. The order of results is not guaranteed.
        /// </para>
        /// <para>
        /// <strong>Example Usage:</strong>
        /// <code>
        /// var partitionIds = new[] { "Order-123", "Order-456", "Order-789" };
        /// var snapshots = await reader.GetManyAsync(partitionIds, cancellationToken);
        ///
        /// foreach (var partitionId in partitionIds)
        /// {
        ///     if (snapshots.TryGetValue(partitionId, out var snapshot))
        ///     {
        ///         // Snapshot exists for this partition
        ///         ProcessSnapshot(snapshot);
        ///     }
        ///     else
        ///     {
        ///         // No snapshot for this partition, need to rebuild from events
        ///         RebuildFromEvents(partitionId);
        ///     }
        /// }
        /// </code>
        /// </para>
        /// </remarks>
        Task<IDictionary<string, SnapshotInfo>> GetManyAsync(
            IEnumerable<string> snapshotPartitionIds,
            CancellationToken cancellationToken
        );
    }
}
