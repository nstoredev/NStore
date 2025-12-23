using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace NStore.Core.Persistence
{
    /// <summary>
    /// Represents a read request for a single partition with its specific index range.
    /// </summary>
    /// <param name="PartitionId">The partition identifier. Must not be null or whitespace.</param>
    /// <param name="FromPartitionIndexInclusive">Starting index (inclusive) within the partition.</param>
    /// <param name="ToPartitionIndexInclusive">Ending index (inclusive) within the partition. Must be >= FromPartitionIndexInclusive.</param>
    public readonly record struct PartitionReadRequest(
        string PartitionId,
        long FromPartitionIndexInclusive,
        long ToPartitionIndexInclusive = long.MaxValue
    );

    /// <summary>
    /// Sometimes it is necessary to query the store not only for
    /// a single partition but for a series of partitions. This is needed
    /// when you want better performance for historical data or if you have
    /// a "single conceptual aggregate" splitted in multiple partitions.
    /// </summary>
    public interface IMultiPartitionPersistenceReader
    {
        /// <summary>
        /// This is the same as <see cref="IPersistence.ReadForwardAsync"/> but for multiple partitions.
        /// </summary>
        /// <param name="partitionIdsList">List of all partition id I want to read.</param>
        /// <param name="fromLowerIndexInclusive"></param>
        /// <param name="subscription"></param>
        /// <param name="toUpperIndexInclusive"></param>
        /// <param name="cancellationToken"></param>
        /// <remarks><para>
        /// We have ZERO guarantee on the order of partition list, we can only assume that
        /// for each partition all chunk are sent to <see cref="ISubscription"/> in correct partition index
        /// order, but we do not have any guarantee on "temporal" ordering between partitions.
        /// </para>
        /// <para>
        /// As an example, lets suppose that temporal order of chunks are (partition id, version id)
        /// (Partition_X, 1), (Partition_Y, 1), (Partition_Z, 1), (Partition_Y, 2), (Partition_Y, 3), (Partition_X, 2)
        /// </para>
        /// <para>
        /// If we search for X and Y we can have both sequences
        /// (Partition_X, 1), (Partition_X, 2), (Partition_Y, 1), (Partition_Y, 2), (Partition_Y, 3)
        /// (Partition_X, 1), (Partition_Y, 1), (Partition_Y, 2), (Partition_Y, 3), (Partition_X, 2)
        /// </para>
        /// <para>
        /// The only guarantee is that we cannot have for the SAME partition an unordered sequence of versions but
        /// no ordering is guaranteed between partitions, even if you perform subsequent calls, each call can
        /// return a different order.
        /// </para>
        /// </remarks>
        Task ReadForwardMultiplePartitionsAsync(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken
        );

#if NET8_0_OR_GREATER
        /// <summary>
        /// This is the same as <see cref="IPersistence.ReadForwardAsync"/> but for multiple partitions.
        /// Returns an IAsyncEnumerable for modern async streaming.
        /// </summary>
        /// <param name="partitionIdsList">List of all partition id I want to read.</param>
        /// <param name="fromLowerIndexInclusive"></param>
        /// <param name="toUpperIndexInclusive"></param>
        /// <param name="cancellationToken"></param>
        /// <remarks><para>
        /// We have ZERO guarantee on the order of partition list, we can only assume that
        /// for each partition all chunks are returned in correct partition index
        /// order, but we do not have any guarantee on "temporal" ordering between partitions.
        /// </para>
        /// <para>
        /// As an example, lets suppose that temporal order of chunks are (partition id, version id)
        /// (Partition_X, 1), (Partition_Y, 1), (Partition_Z, 1), (Partition_Y, 2), (Partition_Y, 3), (Partition_X, 2)
        /// </para>
        /// <para>
        /// If we search for X and Y we can have both sequences
        /// (Partition_X, 1), (Partition_X, 2), (Partition_Y, 1), (Partition_Y, 2), (Partition_Y, 3)
        /// (Partition_X, 1), (Partition_Y, 1), (Partition_Y, 2), (Partition_Y, 3), (Partition_X, 2)
        /// </para>
        /// <para>
        /// The only guarantee is that we cannot have for the SAME partition an unordered sequence of versions but
        /// no ordering is guaranteed between partitions, even if you perform subsequent calls, each call can
        /// return a different order.
        /// </para>
        /// </remarks>
        IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsAsyncEnumerable(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken
        );
#endif

        /// <summary>
        /// Reads multiple partitions where each partition can have its own index range.
        /// </summary>
        /// <param name="partitionRequests">List of partition read requests, each with its own range.</param>
        /// <param name="subscription">Subscriber that will receive chunks.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <remarks>
        /// <para>
        /// <strong>Index Semantics:</strong> All index values (FromPartitionIndexInclusive, ToPartitionIndexInclusive) 
        /// refer to the <strong>partition-local index</strong>, NOT the global position in the store.
        /// </para>
        /// <para>
        /// <strong>Ordering Guarantees:</strong> Chunks within the SAME partition are ordered by partition index.
        /// NO temporal ordering is guaranteed between different partitions (same as ReadForwardMultiplePartitionsAsync).
        /// </para>
        /// <para>
        /// <strong>Use Case:</strong> Designed for IBatchStream where loading multiple aggregates 
        /// requires different historical ranges per aggregate (e.g., load last 10 events from Order-123, 
        /// last 5 events from Customer-456).
        /// </para>
        /// </remarks>
        Task ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            ISubscription subscription,
            CancellationToken cancellationToken
        );

#if NET8_0_OR_GREATER
        /// <summary>
        /// Asynchronously enumerates chunks for multiple partitions where each partition can have its own index range.
        /// Consumers can use `await foreach` and stop enumeration early to signal the producer to stop.
        /// </summary>
        /// <param name="partitionRequests">List of partition read requests, each with its own range.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>An async sequence of <see cref="IChunk"/> instances.</returns>
        IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            CancellationToken cancellationToken = default
        );
#endif
    }
}
