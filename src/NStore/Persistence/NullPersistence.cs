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

        public Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId, CancellationToken cancellationToken)
        {
            return Task.FromResult<IChunk>(null);
        }

        public bool SupportsFillers => false;

        public Task ReadPartitionForward(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
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
            ISubscription subscription,
            long toLowerIndexInclusive, 
            int limit,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task<IChunk> ReadLast(string partitionId, int toUpperIndexInclusive, CancellationToken cancellationToken)
        {
            return Task.FromResult<IChunk>(null);
        }

        public Task ReadAllAsync(long fromSequenceIdInclusive, ISubscription subscription, int limit, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}