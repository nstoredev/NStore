using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public interface IPersistence
    {
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

        Task<IChunk> PeekPartition(
            string partitionId,
            int maxVersion,
            CancellationToken cancellationToken
        );

        Task ReadAllAsync(
            long fromSequenceIdInclusive, 
            ISubscription subscription, 
            int limit, 
            CancellationToken cancellationToken
        );

        Task PersistAsync(
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