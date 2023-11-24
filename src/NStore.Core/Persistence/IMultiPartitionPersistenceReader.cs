using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
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
        /// <param name="fromLowerPositionInclusive">Lower global position to perform a read (included)</param>
        /// <param name="subscription"></param>
        /// <param name="toUpperPositionInclusive">Upper global position to perform a read (included)</param>
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
        Task ReadForwardMultiplePartitionsByGlobalPositionAsync(
            IEnumerable<string> partitionIdsList,
            long fromLowerPositionInclusive,
            ISubscription subscription,
            long toUpperPositionInclusive,
            CancellationToken cancellationToken
        );
    }
}
