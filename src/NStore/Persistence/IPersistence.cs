using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public interface IPersistence
    {
        bool SupportsFillers { get;  }

        Task ReadPartitionForward(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        );

        Task ReadPartitionBackward(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        );

        Task<IChunk> ReadLast(
            string partitionId,
            int toUpperIndexInclusive,
            CancellationToken cancellationToken
        );

        Task ReadAllAsync(
            long fromSequenceIdInclusive, 
            ISubscription subscription, 
            int limit, 
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
        /// <returns></returns>
        Task<IChunk> AppendAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken
        );

        /// @@REVIEW delete invalid stream should throw or not?
        Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken
        );
    }
}