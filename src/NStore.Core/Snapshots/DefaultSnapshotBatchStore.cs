using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Snapshots
{
    /// <summary>
    /// Default implementation of <see cref="ISnapshotBatchStore"/> that performs batch
    /// snapshot operations in parallel using an underlying <see cref="ISnapshotStore"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This implementation wraps an existing <see cref="ISnapshotStore"/> and executes
    /// operations in parallel using <see cref="Task.WhenAll"/>.
    /// It does not perform database-level batch operations; instead, it optimizes throughput
    /// by issuing multiple concurrent requests.
    /// </para>
    /// <para>
    /// This is a general-purpose implementation suitable for most scenarios. For specialized
    /// storage backends that support native batch queries (e.g., SQL with IN clauses, MongoDB
    /// with $in operator), consider implementing a custom <see cref="ISnapshotBatchStore"/>
    /// that leverages those capabilities for better performance.
    /// </para>
    /// </remarks>
    public class DefaultSnapshotBatchStore : ISnapshotBatchStore
    {
        private readonly ISnapshotStore _snapshotStore;

        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultSnapshotBatchStore"/> class.
        /// </summary>
        /// <param name="snapshotStore">The underlying snapshot store to use for individual snapshot operations.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="snapshotStore"/> is null.</exception>
        public DefaultSnapshotBatchStore(ISnapshotStore snapshotStore)
        {
            _snapshotStore = snapshotStore ?? throw new ArgumentNullException(nameof(snapshotStore));
        }

        /// <summary>
        /// Retrieves the most recent snapshots for multiple partitions by executing
        /// parallel calls to <see cref="ISnapshotStore.GetLastAsync"/>.
        /// </summary>
        /// <param name="snapshotPartitionIds">Collection of partition IDs to retrieve snapshots for.</param>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>
        /// A dictionary mapping partition IDs to their corresponding <see cref="SnapshotInfo"/>.
        /// Only partitions that have snapshots are included in the result.
        /// </returns>
        /// <remarks>
        /// <para>
        /// <strong>Implementation Details:</strong>
        /// </para>
        /// <list type="number">
        /// <item><description>Deduplicates the input partition IDs to avoid redundant queries.</description></item>
        /// <item><description>Executes <see cref="ISnapshotStore.GetLastAsync"/> for each partition in parallel.</description></item>
        /// <item><description>Filters out null results (partitions without snapshots).</description></item>
        /// <item><description>Returns a dictionary mapping partition IDs to their snapshots.</description></item>
        /// </list>
        /// <para>
        /// <strong>Concurrency:</strong> All snapshot retrievals execute concurrently. The actual parallelism
        /// depends on the ThreadPool and the underlying store's ability to handle concurrent requests.
        /// </para>
        /// <para>
        /// <strong>Error Handling:</strong> If any individual read fails, the entire operation fails and
        /// throws the exception. Partial results are not returned.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="snapshotPartitionIds"/> is null.</exception>
        public async Task<IDictionary<string, SnapshotInfo>> GetManyAsync(
            IEnumerable<string> snapshotPartitionIds,
            CancellationToken cancellationToken)
        {
            if (snapshotPartitionIds == null)
                throw new ArgumentNullException(nameof(snapshotPartitionIds));

            // Deduplicate partition IDs to avoid redundant queries
            var distinctPartitionIds = snapshotPartitionIds.Distinct().ToList();

            // If no partition IDs provided, return an empty dictionary
            if (distinctPartitionIds.Count == 0)
                return new Dictionary<string, SnapshotInfo>();

            // Execute all GetLastAsync calls in parallel
            var tasks = distinctPartitionIds.Select(async partitionId =>
            {
                var snapshot = await _snapshotStore.GetLastAsync(partitionId, cancellationToken).ConfigureAwait(false);
                return new { PartitionId = partitionId, Snapshot = snapshot };
            });

            var results = await Task.WhenAll(tasks).ConfigureAwait(false);

            // Build dictionary, filtering out null snapshots (partitions without snapshots)
            var dictionary = results
                .Where(r => r.Snapshot != null)
                .ToDictionary(r => r.PartitionId, r => r.Snapshot);

            return dictionary;
        }

        /// <summary>
        /// Stores multiple snapshots in parallel using best-effort semantics.
        /// Individual failures are silently ignored as snapshots are an optimization.
        /// </summary>
        /// <param name="snapshots">Dictionary mapping partition IDs to their snapshot information.</param>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        /// <para>
        /// <strong>Implementation Details:</strong>
        /// </para>
        /// <list type="number">
        /// <item><description>Filters out null or empty snapshots.</description></item>
        /// <item><description>Executes <see cref="ISnapshotStore.AddAsync"/> for each snapshot in parallel.</description></item>
        /// <item><description>Ignores individual failures (best-effort semantics).</description></item>
        /// <item><description>Does not throw exceptions for snapshot save failures.</description></item>
        /// </list>
        /// <para>
        /// <strong>Best-Effort Rationale:</strong> Snapshot persistence is an optimization to avoid
        /// rebuilding state from events. If a snapshot fails to save, the system remains functional
        /// as it can rebuild from the event stream. This approach prioritizes system availability
        /// over snapshot consistency.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="snapshots"/> is null.</exception>
        public async Task AddManyAsync(
            IDictionary<string, SnapshotInfo> snapshots,
            CancellationToken cancellationToken)
        {
            if (snapshots == null)
                throw new ArgumentNullException(nameof(snapshots));

            // Filter out null or empty snapshots
            var validSnapshots = snapshots
                .Where(kvp => kvp.Value != null && !kvp.Value.IsEmpty)
                .ToList();

            if (validSnapshots.Count == 0)
                return;

            // Execute all AddAsync calls in parallel with best-effort semantics
            var tasks = validSnapshots.Select(async kvp =>
            {
                try
                {
                    await _snapshotStore.AddAsync(kvp.Key, kvp.Value, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort: silently ignore failures
                    // Snapshots are an optimization, not critical
                }
            });

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
    }
}
