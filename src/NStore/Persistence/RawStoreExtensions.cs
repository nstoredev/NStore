using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public static class RawStoreExtensions
    {
        public static Task ReadPartitionForward(
            this IPersistence store,
            string partitionId,
            long fromLowerIndexInclusive,
            IPartitionConsumer partitionConsumer
        )
        {
            return store.ReadPartitionForward(
                partitionId,
                fromLowerIndexInclusive,
                partitionConsumer,
                long.MaxValue,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadPartitionForward(
            this IPersistence store,
            string partitionId,
            long fromLowerIndexInclusive,
            IPartitionConsumer partitionConsumer,
            long toUpperIndexInclusive
        )
        {
            return store.ReadPartitionForward(
                partitionId,
                fromLowerIndexInclusive,
                partitionConsumer,
                toUpperIndexInclusive,
                int.MaxValue,
                CancellationToken.None
            );
        }


        public static Task ReadPartitionBackward(
            this IPersistence store,
            string partitionId,
            long fromUpperIndexInclusive,
            IPartitionConsumer partitionConsumer
        )
        {
            return store.ReadPartitionBackward(
                partitionId,
                fromUpperIndexInclusive,
                partitionConsumer,
                0,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ReadPartitionBackward(
            this IPersistence store,
            string partitionId,
            long fromUpperIndexInclusive,
            IPartitionConsumer partitionConsumer,
            long toLowerIndexInclusive
        )
        {
            return store.ReadPartitionBackward(
                partitionId,
                fromUpperIndexInclusive,
                partitionConsumer,
                toLowerIndexInclusive,
                int.MaxValue,
                CancellationToken.None
            );
        }

        public static Task ScanStoreAsync(
            this IPersistence store,
            long sequenceStart,
            ScanDirection direction,
            IStoreConsumer consumer
        )
        {
            return store.ScanStoreAsync(sequenceStart, direction, consumer, int.MaxValue, CancellationToken.None);
        }

        public static Task ScanStoreAsync(
            this IPersistence store,
            long sequenceStart,
            ScanDirection direction,
            IStoreConsumer consumer,
            int limit
        )
        {
            return store.ScanStoreAsync(sequenceStart, direction, consumer, limit, CancellationToken.None);
        }

        public static Task PersistAsync(
            this IPersistence store,
            string partitionId,
            long index,
            object payload
        )
        {
            return store.PersistAsync(partitionId, index, payload, null, CancellationToken.None);
        }

        public static Task PersistAsync(
            this IPersistence store,
            string partitionId,
            long index,
            object payload,
            string operationId
        )
        {
            return store.PersistAsync(partitionId, index, payload, operationId, CancellationToken.None);
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