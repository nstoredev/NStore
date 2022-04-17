using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public static class StoreExtensions
    {
        public static Task ReadForwardAsync(
            this IStore store,
            string partitionId,
            ISubscription subscription
        )
        {
            return store.ReadForwardAsync(
                partitionId,
                0,
                subscription,
                long.MaxValue,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadForwardAsync(
            this IStore store,
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription
        )
        {
            return store.ReadForwardAsync(
                partitionId,
                fromLowerIndexInclusive,
                subscription,
                long.MaxValue,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadForwardAsync(
            this IStore store,
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive
        )
        {
            return store.ReadForwardAsync(
                partitionId,
                fromLowerIndexInclusive,
                subscription,
                toUpperIndexInclusive,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadBackwardAsync(
            this IStore store,
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription
        )
        {
            return store.ReadBackwardAsync(
                partitionId,
                fromUpperIndexInclusive,
                subscription,
                0,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadBackwardAsync(
            this IStore store,
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive
        )
        {
            return store.ReadBackwardAsync(
                partitionId,
                fromUpperIndexInclusive,
                subscription,
                toLowerIndexInclusive,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadAllAsync(
            this IStore store,
            long fromPositionInclusive,
            ISubscription subscription
        )
        {
            return store.ReadAllAsync(fromPositionInclusive, subscription, int.MaxValue, CancellationToken.None);
        }

        public static Task ReadAllAsync(
            this IStore store,
            long fromPositionInclusive,
            ISubscription subscription,
            int limit
        )
        {
            return store.ReadAllAsync(fromPositionInclusive, subscription, limit, CancellationToken.None);
        }

        public static Task<IChunk> ReadSingleBackwardAsync(
            this IStore store,
            string partitionId
        )
        {
            return store.ReadSingleBackwardAsync(partitionId, long.MaxValue, CancellationToken.None);
        }

        public static Task<IChunk> ReadSingleBackwardAsync(
            this IStore store,
            string partitionId,
            CancellationToken cancellationToken
        )
        {
            return store.ReadSingleBackwardAsync(partitionId, long.MaxValue, cancellationToken);
        }

        public static Task<IChunk> AppendAsync(
            this IStore store,
            string partitionId,
            long index,
            object payload
        )
        {
            return store.AppendAsync(partitionId, index, payload, null, CancellationToken.None);
        }
        
        public static Task<IChunk> AppendAsync(
            this IStore store,
            string partitionId,
            long index,
            object payload,
            string operationId
        )
        {
            return store.AppendAsync(partitionId, index, payload, operationId, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IStore store,
            string partitionId
        )
        {
            return store.DeleteAsync(partitionId, 0, long.MaxValue, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IStore store,
            string partitionId,
            long fromLowerIndexInclusive
        )
        {
            return store.DeleteAsync(partitionId, fromLowerIndexInclusive, long.MaxValue, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IStore store,
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive
        )
        {
            return store.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive, CancellationToken.None);
        }

        public static Task<long> ReadLastPositionAsync(this IStore store)
        {
            return store.ReadLastPositionAsync(CancellationToken.None);
        }

        public static Task<IChunk> ReadByOpeationIdAsync(
            this IStore store,
            string partitionId,
            string operationId
        )
        {
            return store.ReadByOperationIdAsync(partitionId, operationId, CancellationToken.None);
        }

        public static Task ReadAllByOperationIdAsync(
            this IStore store,
            string operationId,
            ISubscription subscription
        )
        {
            return store.ReadAllByOperationIdAsync(operationId, subscription, CancellationToken.None);
        }
        
        public static Task<IChunk> ReplaceOneAsync(
            this IStore store,
            long position,
            string partitionId,
            long index,
            object payload
        )
        {
            return store.ReplaceOneAsync(position, partitionId, index, payload, null, CancellationToken.None);
        }

        public static Task<IChunk> ReadOneAsync(
            this IStore store,
            long position
        )
        {
            return store.ReadOneAsync(position, CancellationToken.None);
        }
    }
}