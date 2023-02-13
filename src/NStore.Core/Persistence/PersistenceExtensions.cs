using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    /// <summary>
    /// Extension functions that allow for a simpler API than using the
    /// basic <see cref="IPersistence.ReadForwardAsync"/> base function.
    /// </summary>
    public static class PersistenceExtensions
    {
        public static Task ReadForwardAsync(
            this IPersistence persistence,
            string partitionId,
            ISubscription subscription
        )
        {
            return persistence.ReadForwardAsync(
                partitionId,
                0,
                subscription,
                long.MaxValue,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadForwardMultiplePartitionsAsync(
            this IMultiPartitionPersistenceReader persistence,
            IEnumerable<string> partitionIdsList,
            ISubscription subscription
        )
        {
            return persistence.ReadForwardMultiplePartitionsAsync(
                partitionIdsList: partitionIdsList,
                fromLowerIndexInclusive: 0,
                subscription: subscription,
                toUpperIndexInclusive: long.MaxValue,
                cancellationToken: CancellationToken.None
            );
        }

        public static Task ReadForwardAsync(
            this IPersistence persistence,
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription
        )
        {
            return persistence.ReadForwardAsync(
                partitionId,
                fromLowerIndexInclusive,
                subscription,
                long.MaxValue,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadForwardMultiplePartitionsAsync(
            this IMultiPartitionPersistenceReader persistence,
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            ISubscription subscription
        )
        {
            return persistence.ReadForwardMultiplePartitionsAsync(
                partitionIdsList: partitionIdsList,
                fromLowerIndexInclusive: fromLowerIndexInclusive,
                subscription: subscription,
                toUpperIndexInclusive: long.MaxValue,
                cancellationToken: CancellationToken.None
            );
        }

        public static Task ReadForwardAsync(
            this IPersistence persistence,
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive
        )
        {
            return persistence.ReadForwardAsync(
                partitionId,
                fromLowerIndexInclusive,
                subscription,
                toUpperIndexInclusive,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadForwardMultiplePartitionsAsync(
            this IMultiPartitionPersistenceReader persistence,
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive
        )
        {
            return persistence.ReadForwardMultiplePartitionsAsync(
                partitionIdsList,
                fromLowerIndexInclusive: fromLowerIndexInclusive,
                subscription: subscription,
                toUpperIndexInclusive: toUpperIndexInclusive,
                cancellationToken: CancellationToken.None
            );
        }

        public static Task ReadBackwardAsync(
            this IPersistence persistence,
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription
        )
        {
            return persistence.ReadBackwardAsync(
                partitionId,
                fromUpperIndexInclusive,
                subscription,
                0,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadBackwardAsync(
            this IPersistence persistence,
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive
        )
        {
            return persistence.ReadBackwardAsync(
                partitionId,
                fromUpperIndexInclusive,
                subscription,
                toLowerIndexInclusive,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadAllAsync(
            this IPersistence persistence,
            long fromPositionInclusive,
            ISubscription subscription
        )
        {
            return persistence.ReadAllAsync(fromPositionInclusive, subscription, int.MaxValue, CancellationToken.None);
        }

        public static Task ReadAllAsync(
            this IPersistence persistence,
            long fromPositionInclusive,
            ISubscription subscription,
            int limit
        )
        {
            return persistence.ReadAllAsync(fromPositionInclusive, subscription, limit, CancellationToken.None);
        }

        public static Task<IChunk> ReadSingleBackwardAsync(
            this IPersistence persistence,
            string partitionId
        )
        {
            return persistence.ReadSingleBackwardAsync(partitionId, long.MaxValue, CancellationToken.None);
        }

        public static Task<IChunk> ReadSingleBackwardAsync(
            this IPersistence persistence,
            string partitionId,
            CancellationToken cancellationToken
        )
        {
            return persistence.ReadSingleBackwardAsync(partitionId, long.MaxValue, cancellationToken);
        }

        public static Task<IChunk> AppendAsync(
            this IPersistence persistence,
            string partitionId,
            long index,
            object payload
        )
        {
            return persistence.AppendAsync(partitionId, index, payload, null, CancellationToken.None);
        }

        public static Task<IChunk> AppendAsync(
            this IPersistence persistence,
            string partitionId,
            long index,
            object payload,
            string operationId
        )
        {
            return persistence.AppendAsync(partitionId, index, payload, operationId, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IPersistence persistence,
            string partitionId
        )
        {
            return persistence.DeleteAsync(partitionId, 0, long.MaxValue, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IPersistence persistence,
            string partitionId,
            long fromLowerIndexInclusive
        )
        {
            return persistence.DeleteAsync(partitionId, fromLowerIndexInclusive, long.MaxValue, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IPersistence persistence,
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive
        )
        {
            return persistence.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive, CancellationToken.None);
        }

        public static Task<long> ReadLastPositionAsync(this IPersistence persistence)
        {
            return persistence.ReadLastPositionAsync(CancellationToken.None);
        }

        public static Task<IChunk> ReadByOpeationIdAsync(
            this IPersistence persistence,
            string partitionId,
            string operationId
        )
        {
            return persistence.ReadByOperationIdAsync(partitionId, operationId, CancellationToken.None);
        }

        public static Task ReadAllByOperationIdAsync(
            this IPersistence persistence,
            string operationId,
            ISubscription subscription
        )
        {
            return persistence.ReadAllByOperationIdAsync(operationId, subscription, CancellationToken.None);
        }

        public static Task<IChunk> ReplaceOneAsync(
            this IPersistence persistence,
            long position,
            string partitionId,
            long index,
            object payload
        )
        {
            return persistence.ReplaceOneAsync(position, partitionId, index, payload, null, CancellationToken.None);
        }

        public static Task<IChunk> ReadOneAsync(
            this IPersistence persistence,
            long position
        )
        {
            return persistence.ReadOneAsync(position, CancellationToken.None);
        }
    }
}