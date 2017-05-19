using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
    public class NullStore : IRawStore
    {
        public Task DeleteAsync(string partitionId, long fromIndex = 0, long toIndex = long.MaxValue, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.FromResult(0);
        }

        public Task PersistAsync(string partitionId, long index, object payload, string operationId = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.FromResult(0);
        }

        public Task ScanPartitionAsync(string partitionId, long fromIndexInclusive, ScanDirection direction, IPartitionObserver partitionObserver, long toIndexInclusive = long.MaxValue, int limit = int.MaxValue, CancellationToken cancellationToken = default(CancellationToken))
        {
			return Task.FromResult(0);
        }

        public Task ScanStoreAsync(long sequenceStart, ScanDirection direction, IStoreObserver observer, int limit = int.MaxValue, CancellationToken cancellationToken = default(CancellationToken))
        {
			return Task.FromResult(0);
        }
    }
}