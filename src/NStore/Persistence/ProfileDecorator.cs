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
            ReadLastCounter = new TaskProfilingInfo("Read last position", "lastposition");
            PeekCounter = new TaskProfilingInfo("Peek");
        }

        public TaskProfilingInfo PersistCounter { get; }
        public TaskProfilingInfo PeekCounter { get; }
        public TaskProfilingInfo DeleteCounter { get; }
        public TaskProfilingInfo StoreScanCounter { get; }
        public TaskProfilingInfo ReadLastCounter { get; }
        public TaskProfilingInfo PartitionReadForwardCounter { get; }
        public TaskProfilingInfo PartitionReadBackwardCounter { get; }

        public bool SupportsFillers => _store.SupportsFillers;

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
                )).ConfigureAwait(false);
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
                )).ConfigureAwait(false);
        }

        public Task<IChunk> ReadLast(string partitionId, long toUpperIndexInclusive, CancellationToken cancellationToken)
        {
            return PeekCounter.CaptureAsync(() =>
                _store.ReadLast(partitionId, toUpperIndexInclusive, cancellationToken)
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
            ).ConfigureAwait(false);
        }

        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            return await ReadLastCounter.CaptureAsync(()=>
                _store.ReadLastPositionAsync(cancellationToken)
            ).ConfigureAwait(false);
        }

        public Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId, CancellationToken cancellationToken)
        {
            return PersistCounter.CaptureAsync(() =>
                _store.AppendAsync(partitionId, index, payload, operationId, cancellationToken)
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
            ).ConfigureAwait(false);
        }
    }
}