using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public static class PersistenceExtensions
    {
        public static Task ReadPartitionForward(
            this IPersistence store,
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
        
        public static Task ReadPartitionForward(
            this IPersistence store,
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

        public static Task ReadPartitionForward(
            this IPersistence store,
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


        public static Task ReadPartitionBackward(
            this IPersistence store,
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

        public static Task ReadPartitionBackward(
            this IPersistence store,
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
            this IPersistence store,
            long sequenceStart,
            ISubscription subscription
        )
        {
            return store.ReadAllAsync(sequenceStart, subscription, int.MaxValue, CancellationToken.None);
        }

        public static Task ReadAllAsync(
            this IPersistence store,
            long sequenceStart,
            ISubscription subscription,
            int limit
        )
        {
            return store.ReadAllAsync(sequenceStart, subscription, limit, CancellationToken.None);
        }

        public static Task<IChunk> ReadLast(this IPersistence store, string partitionId)
        {
            return store.ReadSingleBackwardAsync(partitionId, long.MaxValue, CancellationToken.None);
        }

        public static Task<IChunk> AppendAsync(
            this IPersistence store,
            string partitionId,
            object payload
        )
        {
            return store.AppendAsync(partitionId, -1, payload, null, CancellationToken.None);
        }

        public static Task<IChunk> AppendAsync(
            this IPersistence store,
            string partitionId,
            long index,
            object payload
        )
        {
            return store.AppendAsync(partitionId, index, payload, null, CancellationToken.None);
        }

        public static Task<IChunk> AppendAsync(
            this IPersistence store,
            string partitionId,
            long index,
            object payload,
            string operationId
        )
        {
            return store.AppendAsync(partitionId, index, payload, operationId, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IPersistence store,
            string partitionId
        )
        {
            return store.DeleteAsync(partitionId, 0, long.MaxValue, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IPersistence store,
            string partitionId,
            long fromLowerIndexInclusive
        )
        {
            return store.DeleteAsync(partitionId, fromLowerIndexInclusive, long.MaxValue, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IPersistence store,
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive
        )
        {
            return store.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive, CancellationToken.None);
        }
    }
}