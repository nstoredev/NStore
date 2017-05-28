using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
    public class NullStore : IRawStore
    {
        public Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task PersistAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task ReadPartitionForward(
            string partitionId,
            long fromLowerIndexInclusive,
            IPartitionConsumer partitionConsumer,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }

        public Task ReadPartitionBackward(
            string partitionId, 
            long fromUpperIndexInclusive, 
            IPartitionConsumer partitionConsumer,
            long toLowerIndexInclusive, 
            int limit,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task ScanStoreAsync(
            long fromSequenceIdInclusive, 
            ScanDirection direction, 
            IStoreConsumer consumer, 
            int limit, 
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}