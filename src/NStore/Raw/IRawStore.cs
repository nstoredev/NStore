using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
    public interface IRawStore
    {
        Task ReadPartitionForward(
            string partitionId,
            long fromLowerIndexInclusive,
            IPartitionConsumer partitionConsumer,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        );

        Task ReadPartitionBackward(
            string partitionId,
            long fromUpperIndexInclusive,
            IPartitionConsumer partitionConsumer,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        );

        Task ScanStoreAsync(
            long sequenceStart,
            ScanDirection direction,
            IStoreConsumer consumer,
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
            long fromIndex ,
            long toIndex ,
            CancellationToken cancellationToken 
        );
    }


    public static class RawStoreExtensions
    {
        public static Task ReadPartitionForward(
            this IRawStore store,
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
            this IRawStore store,
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
            this IRawStore store,
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
            this IRawStore store,
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
            this IRawStore store,
            long sequenceStart,
            ScanDirection direction,
            IStoreConsumer consumer
        )
        {
            return store.ScanStoreAsync(sequenceStart, direction, consumer, int.MaxValue, CancellationToken.None);
        }

        public static Task ScanStoreAsync(
            this IRawStore store,
            long sequenceStart,
            ScanDirection direction,
            IStoreConsumer consumer,
            int limit
        )
        {
            return store.ScanStoreAsync(sequenceStart, direction, consumer, limit, CancellationToken.None);
        }

        public static Task PersistAsync(
            this IRawStore store,
            string partitionId,
            long index,
            object payload
        )
        {
            return store.PersistAsync(partitionId, index, payload, null, CancellationToken.None);
        }

        public static Task PersistAsync(
            this IRawStore store,
            string partitionId,
            long index,
            object payload,
            string operationId
        )
        {
            return store.PersistAsync(partitionId, index, payload, operationId, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IRawStore store,
            string partitionId
        )
        {
            return store.DeleteAsync(partitionId, 0, long.MaxValue, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IRawStore store,
            string partitionId,
            long fromLowerIndexInclusive
        )
        {
            return store.DeleteAsync(partitionId, fromLowerIndexInclusive, long.MaxValue, CancellationToken.None);
        }

        public static Task DeleteAsync(
            this IRawStore store,
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive
        )
        {
            return store.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive, CancellationToken.None);
        }
    }
}