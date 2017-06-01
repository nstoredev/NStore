using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class NullPersistence : IPersistence
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

        public Task<IPartitionData> PeekPartition(string partitionId, int maxVersion, CancellationToken cancellationToken)
        {
            return Task.FromResult<IPartitionData>(null);
        }

        public Task ReadAllAsync(
            long fromSequenceIdInclusive, 
            ReadDirection direction, 
            IAllPartitionsConsumer consumer, 
            int limit, 
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}