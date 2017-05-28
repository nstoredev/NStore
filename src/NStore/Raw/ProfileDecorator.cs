using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
    public class ProfileDecorator : IRawStore
    {
        private readonly IRawStore _store;

        public ProfileDecorator(IRawStore store)
        {
            _store = store;
            PersistCounter = new TaskProfilingInfo("Persist");
            PartitionReadForwardCounter = new TaskProfilingInfo("Partition read forward", "chunks read");
            PartitionReadBackwardCounter = new TaskProfilingInfo("Partition read backward", "chunks read");
            DeleteCounter = new TaskProfilingInfo("Delete");
            StoreScanCounter = new TaskProfilingInfo("Store Scan", "chunks read");
        }

        public TaskProfilingInfo PersistCounter { get; }
        public TaskProfilingInfo DeleteCounter { get; }
        public TaskProfilingInfo StoreScanCounter { get; }
        public TaskProfilingInfo PartitionReadForwardCounter { get; }
        public TaskProfilingInfo PartitionReadBackwardCounter { get; }

        public async Task ReadPartitionForward(
            string partitionId,
            long fromLowerIndexInclusive,
            IPartitionConsumer partitionConsumer,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
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

        public async Task ReadPartitionBackward(
            string partitionId,
            long fromUpperIndexInclusive,
            IPartitionConsumer partitionConsumer,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
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

        public async Task ScanStoreAsync(
            long fromSequenceIdInclusive,
            ScanDirection direction,
            IStoreConsumer consumer,
            int limit,
            CancellationToken cancellationToken)
        {
            var storeObserver = new LambdaStoreConsumer((si, s, l, o) =>
            {
                StoreScanCounter.IncCounter1();
                return consumer.Consume(si, s, l, o);
            });

            await StoreScanCounter.CaptureAsync(() =>
                _store.ScanStoreAsync(fromSequenceIdInclusive, direction, storeObserver, limit, cancellationToken)
            );
        }

        public async Task PersistAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            await PersistCounter.CaptureAsync(() =>
                _store.PersistAsync(partitionId, index, payload, operationId, cancellationToken)
            );
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            await DeleteCounter.CaptureAsync(() =>
                _store.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive, cancellationToken)
            );
        }
    }
}