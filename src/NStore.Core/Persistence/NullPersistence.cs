using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
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