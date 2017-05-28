using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
    public class ProfileDecorator : IRawStore
    {
        private readonly IRawStore _store;

        public TaskProfilingInfo PersistCounter { get; }
        public TaskProfilingInfo DeleteCounter { get; }
        public TaskProfilingInfo StoreScanCounter { get; }
        public TaskProfilingInfo PartitionReadForwardCounter { get; }
        public TaskProfilingInfo PartitionReadBackwardCounter { get; }

        public ProfileDecorator(IRawStore store)
        {
            _store = store;
            PersistCounter = new TaskProfilingInfo("Persist");
            PartitionReadForwardCounter = new TaskProfilingInfo("Partition read forward", "chunks read");
            PartitionReadBackwardCounter = new TaskProfilingInfo("Partition read backward", "chunks read");
            DeleteCounter = new TaskProfilingInfo("Delete");
            StoreScanCounter = new TaskProfilingInfo("Store Scan", "chunks read");
        }

        public Task ReadPartitionForward(string partitionId, long fromLowerIndexInclusive, IPartitionConsumer partitionConsumer)
        {
            return ReadPartitionForward(
                partitionId, 
                fromLowerIndexInclusive, 
                partitionConsumer, 
                long.MaxValue,
                int.MaxValue, 
                CancellationToken.None
            );
        }

        public Task ReadPartitionForward(string partitionId, long fromLowerIndexInclusive, IPartitionConsumer partitionConsumer,
            long toUpperIndexInclusive)
        {
            return ReadPartitionForward(
                partitionId, 
                fromLowerIndexInclusive, 
                partitionConsumer, 
                toUpperIndexInclusive,
                int.MaxValue, 
                CancellationToken.None
            );
        }

        public async Task ReadPartitionForward(string partitionId, long fromLowerIndexInclusive,
            IPartitionConsumer partitionConsumer, long toUpperIndexInclusive = Int64.MaxValue, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var counter = new LambdaPartitionConsumer((l, o) =>
            {
                PartitionReadForwardCounter.IncCounter1();
                return partitionConsumer.Consume(l, o);
            });

            await PartitionReadForwardCounter.CaptureAsync(() =>
                _store.ReadPartitionForward(
                    partitionId,
                    fromLowerIndexInclusive,
                    counter,
                    toUpperIndexInclusive,
                    limit,
                    cancellationToken
                ));
        }

        public async Task ReadPartitionBackward(string partitionId, long fromUpperIndexInclusive,
            IPartitionConsumer partitionConsumer, long toLowerIndexInclusive = Int64.MaxValue, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var counter = new LambdaPartitionConsumer((l, o) =>
            {
                PartitionReadBackwardCounter.IncCounter1();
                return partitionConsumer.Consume(l, o);
            });

            await PartitionReadBackwardCounter.CaptureAsync(() =>
                _store.ReadPartitionBackward(
                    partitionId,
                    fromUpperIndexInclusive,
                    counter,
                    toLowerIndexInclusive,
                    limit,
                    cancellationToken
                ));
        }

        public async Task ScanStoreAsync(long sequenceStart, ScanDirection direction, IStoreConsumer consumer,
            int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var storeObserver = new LambdaStoreConsumer((si, s, l, o) =>
            {
                StoreScanCounter.IncCounter1();
                return consumer.Consume(si, s, l, o);
            });

            await StoreScanCounter.CaptureAsync(() =>
                _store.ScanStoreAsync(sequenceStart, direction, storeObserver, limit, cancellationToken)
            );
        }

        public async Task PersistAsync(string partitionId, long index, object payload, string operationId = null,
            CancellationToken cancellationToken = new CancellationToken())
        {
            await PersistCounter.CaptureAsync(() =>
                _store.PersistAsync(partitionId, index, payload, operationId, cancellationToken)
            );
        }

        public async Task DeleteAsync(string partitionId, long fromIndex = 0, long toIndex = Int64.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            await DeleteCounter.CaptureAsync(() =>
                _store.DeleteAsync(partitionId, fromIndex, toIndex, cancellationToken)
            );
        }
    }
}