using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public interface IPartitionStore
    {
        bool SupportsFillers { get; }

        Task ReadForwardAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        );

        Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        );

        /// Rename to ReadFirstBackwardAsync?
        Task<IChunk> ReadSingleBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Appends a chunk in the global store
        /// </summary>
        /// <param name="partitionId"></param>
        /// <param name="index"></param>
        /// <param name="payload"></param>
        /// <param name="operationId"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>Chunk appended, or null if idempotency of command does not save anything. </returns>
        ///
        /// Rename to AddAsync / WriteAsync ?
        /// 
        Task<IChunk> AppendAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken
        );


        Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken
        );

        Task<IChunk> ReadByOperationIdAsync(
            string partitionId,
            string operationId,
            CancellationToken cancellationToken
        );
    }
}