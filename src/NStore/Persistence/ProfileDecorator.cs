using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class ProfileDecorator : IPersistence
    {
        private readonly IPersistence _store;

        public ProfileDecorator(IPersistence store)
        {
            _store = store;
            PersistCounter = new TaskProfilingInfo("Persist");
            PartitionReadForwardCounter = new TaskProfilingInfo("Partition read forward", "chunks read");
            PartitionReadBackwardCounter = new TaskProfilingInfo("Partition read backward", "chunks read");
            DeleteCounter = new TaskProfilingInfo("Delete");
            StoreScanCounter = new TaskProfilingInfo("Store Scan", "chunks read");
            PeekCounter = new TaskProfilingInfo("Peek");
        }

        public TaskProfilingInfo PersistCounter { get; }
        public TaskProfilingInfo PeekCounter { get; }
        public TaskProfilingInfo DeleteCounter { get; }
        public TaskProfilingInfo StoreScanCounter { get; }
        public TaskProfilingInfo PartitionReadForwardCounter { get; }
        public TaskProfilingInfo PartitionReadBackwardCounter { get; }

        public async Task ReadPartitionForward(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            var counter = new SubscriptionWrapper(subscription)
            {
                BeforeOnNext = data => PartitionReadForwardCounter.IncCounter1()
            };

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
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            var counter = new SubscriptionWrapper(subscription)
            {
                BeforeOnNext = data => PartitionReadBackwardCounter.IncCounter1()
            };

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

        public Task<IChunk> ReadLast(string partitionId, int upToIndexInclusive, CancellationToken cancellationToken)
        {
            return PeekCounter.CaptureAsync(() =>
                _store.ReadLast(partitionId, upToIndexInclusive, cancellationToken)
            );
        }

        public async Task ReadAllAsync(long fromSequenceIdInclusive, ISubscription subscription, int limit, CancellationToken cancellationToken)
        {
            var wrapper = new SubscriptionWrapper(subscription)
            {
                BeforeOnNext = d => StoreScanCounter.IncCounter1()
            };

            await StoreScanCounter.CaptureAsync(() =>
                _store.ReadAllAsync(fromSequenceIdInclusive, wrapper, limit, cancellationToken)
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