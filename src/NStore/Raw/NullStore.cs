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

        public Task ReadPartitionForward(string partitionId, long fromLowerIndexInclusive, IPartitionConsumer partitionConsumer,
            long toUpperIndexInclusive = Int64.MaxValue, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return Task.CompletedTask;
        }

        public Task ReadPartitionBackward(string partitionId, long fromUpperIndexInclusive, IPartitionConsumer partitionConsumer,
            long toLowerIndexInclusive = Int64.MaxValue, int limit = Int32.MaxValue,
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