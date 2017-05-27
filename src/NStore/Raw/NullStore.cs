using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
    public class NullStore : IRawStore
    {
        public Task DeleteAsync(string partitionId, long fromIndex = 0, long toIndex = long.MaxValue, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.CompletedTask;
        }

        public Task PersistAsync(string partitionId, long index, object payload, string operationId = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.CompletedTask;
        }

        public Task ReadPartitionForward(string partitionId, long fromIndexInclusive, IPartitionConsumer partitionConsumer,
            long toIndexInclusive = Int64.MaxValue, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return Task.CompletedTask;
        }

        public Task ReadPartitionBackward(string partitionId, long fromIndexInclusive, IPartitionConsumer partitionConsumer,
            long toIndexInclusive = Int64.MaxValue, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return Task.CompletedTask;
        }

        public Task ScanStoreAsync(long sequenceStart, ScanDirection direction, IStoreConsumer consumer, int limit = int.MaxValue, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.CompletedTask;
        }
    }
}