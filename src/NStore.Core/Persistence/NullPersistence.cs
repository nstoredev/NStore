using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public class NullPersistence : IPersistence
    {
        public Task<IChunk> ReplaceOneAsync(long position, string partitionId, long index, object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<IChunk>(null);
        }

        public Task<IChunk> ReadOneAsync(long position, CancellationToken cancellationToken)
        {
            return Task.FromResult<IChunk>(null);
        }

        public Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task<IChunk> ReadByOperationIdAsync(string partitionId, string operationId, CancellationToken cancellationToken)
        {
            return Task.FromResult<IChunk>(null);
        }

        public Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(0L);
        }

        public Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId, CancellationToken cancellationToken)
        {
            return Task.FromResult<IChunk>(null);
        }

        public bool SupportsFillers => false;

        public Task ReadForwardAsync(
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

        public Task ReadForwardMultiplePartitionsAsync(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive, CancellationToken cancellationToken)
        {
            return Task.FromResult<IChunk>(null);
        }

        public Task ReadAllAsync(long fromPositionInclusive, ISubscription subscription, int limit, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}