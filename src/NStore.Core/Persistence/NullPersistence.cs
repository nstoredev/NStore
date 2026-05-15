using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public class NullPersistence : IPersistence
    {
        public IReadOnlyList<IChunk> ReadForward(string partitionId, long fromLowerIndexInclusive, long toUpperIndexInclusive, int limit)
        {
            return Array.Empty<IChunk>();
        }

        public IReadOnlyList<IChunk> ReadBackward(string partitionId, long fromUpperIndexInclusive, long toLowerIndexInclusive, int limit)
        {
            return Array.Empty<IChunk>();
        }

        public IChunk ReadSingleBackward(string partitionId, long fromUpperIndexInclusive)
        {
            return null;
        }

        public IChunk ReadByOperationId(string partitionId, string operationId)
        {
            return null;
        }

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

#if NET8_0_OR_GREATER
        public async IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsAsyncEnumerable(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }
#endif

        public Task ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException();
        }

        public IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public Task ReadManyBackwardAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

#if NET8_0_OR_GREATER
        public async IAsyncEnumerable<IChunk> ReadManyBackwardAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
#endif

        public Task<IReadOnlyDictionary<string, IChunk>> ReadLastChunkForPartitionsAsync(
            IEnumerable<string> partitionIds,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<IReadOnlyDictionary<string, IChunk>>(new Dictionary<string, IChunk>());
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