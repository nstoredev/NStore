using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    /// <summary>
    /// Persistence of a partition, it contains all basic operations that we want 
    /// to perform over Partitions.
    /// </summary>
    public interface IPartitionPersistence
    {
        bool SupportsFillers { get; }

        /// <summary>
        /// Basic read operation, read a SINGLE partition id from the lower index to the upper index using
        /// an <see cref="ISubscription"/> to receive the events.
        /// </summary>
        /// <param name="partitionId"></param>
        /// <param name="fromLowerIndexInclusive"></param>
        /// <param name="subscription"></param>
        /// <param name="toUpperIndexInclusive"></param>
        /// <param name="limit"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task ReadForwardAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Read backward, to allow for searching most recent event in a partition.
        /// </summary>
        /// <param name="partitionId"></param>
        /// <param name="fromUpperIndexInclusive"></param>
        /// <param name="subscription"></param>
        /// <param name="toLowerIndexInclusive"></param>
        /// <param name="limit"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Read lastest chunk in a partition, useful to "peek" in a 
        /// partition.
        /// </summary>
        /// <param name="partitionId"></param>
        /// <param name="fromUpperIndexInclusive"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<IChunk> ReadSingleBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Appends a chunk in the global store.
        /// TODO: Rename to AddAsync / WriteAsync ?
        /// </summary>
        /// <param name="partitionId"></param>
        /// <param name="index"></param>
        /// <param name="payload"></param>
        /// <param name="operationId"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>Chunk appended, or null if idempotency of command does not save anything. </returns>
        Task<IChunk> AppendAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Remove all Chunks for a partition in a given index range, useful to remove
        /// older chunks no more needed or to support low level snapshots where we need
        /// to replace a range of chunks with a single one.
        /// </summary>
        /// <param name="partitionId"></param>
        /// <param name="fromLowerIndexInclusive"></param>
        /// <param name="toUpperIndexInclusive"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Read in a single partition for an operation id useful to support idempotency.
        /// </summary>
        /// <param name="partitionId"></param>
        /// <param name="operationId"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<IChunk> ReadByOperationIdAsync(
            string partitionId,
            string operationId,
            CancellationToken cancellationToken
        );
    }
}