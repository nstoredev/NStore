using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public class ProfileDecorator : IPersistence
    {
        private readonly IPersistence _persistence;

        public ProfileDecorator(IPersistence persistence)
        {
            _persistence = persistence;
            PersistCounter = new TaskProfilingInfo("Persist");
            ReadForwardCounter = new TaskProfilingInfo("Partition read forward", "chunks read");
            ReadBackwardCounter = new TaskProfilingInfo("Partition read backward", "chunks read");
            DeleteCounter = new TaskProfilingInfo("Delete");
            StoreScanCounter = new TaskProfilingInfo("Store Scan", "chunks read");
            ReadLastCounter = new TaskProfilingInfo("Read last position", "lastposition");
            ReadSingleBackwardCounter = new TaskProfilingInfo("Read single backward");
            ReadOneCounter = new TaskProfilingInfo("Read one");
            ReplaceOneCounter = new TaskProfilingInfo("Replace one");
        }

        public TaskProfilingInfo PersistCounter { get; }
        public TaskProfilingInfo ReplaceOneCounter { get; }
        public TaskProfilingInfo ReadOneCounter { get; }
        public TaskProfilingInfo ReadSingleBackwardCounter { get; }
        public TaskProfilingInfo DeleteCounter { get; }
        public TaskProfilingInfo StoreScanCounter { get; }
        public TaskProfilingInfo ReadLastCounter { get; }
        public TaskProfilingInfo ReadForwardCounter { get; }
        public TaskProfilingInfo ReadBackwardCounter { get; }

        public bool SupportsFillers => _persistence.SupportsFillers;

        public async Task ReadForwardAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            var counter = new SubscriptionWrapper(subscription)
            {
                BeforeOnNext = data => ReadForwardCounter.IncCounter1()
            };

            await ReadForwardCounter.CaptureAsync(() =>
                _persistence.ReadForwardAsync(
                    partitionId,
                    fromLowerIndexInclusive,
                    counter,
                    toUpperIndexInclusive,
                    limit,
                    cancellationToken
                )).ConfigureAwait(false);
        }

        public async Task ReadForwardMultiplePartitionsByGlobalPositionAsync(
            IEnumerable<string> partitionIdsList,
            long fromLowerPositionInclusive,
            ISubscription subscription,
            long toUpperPositionInclusive,
            CancellationToken cancellationToken)
        {
            var counter = new SubscriptionWrapper(subscription)
            {
                BeforeOnNext = data => ReadForwardCounter.IncCounter1()
            };

            await ReadForwardCounter.CaptureAsync(() =>
                _persistence.ReadForwardMultiplePartitionsByGlobalPositionAsync(
                    partitionIdsList,
                    fromLowerPositionInclusive,
                    counter,
                    toUpperPositionInclusive,
                    cancellationToken
                )).ConfigureAwait(false);
        }

        public async Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            var counter = new SubscriptionWrapper(subscription)
            {
                BeforeOnNext = data => ReadBackwardCounter.IncCounter1()
            };

            await ReadBackwardCounter.CaptureAsync(() =>
                _persistence.ReadBackwardAsync(
                    partitionId,
                    fromUpperIndexInclusive,
                    counter,
                    toLowerIndexInclusive,
                    limit,
                    cancellationToken
                )).ConfigureAwait(false);
        }

        public Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive, CancellationToken cancellationToken)
        {
            return ReadSingleBackwardCounter.CaptureAsync(() =>
                _persistence.ReadSingleBackwardAsync(partitionId, fromUpperIndexInclusive, cancellationToken)
            );
        }

        public async Task ReadAllAsync(long fromPositionInclusive, ISubscription subscription, int limit, CancellationToken cancellationToken)
        {
            var wrapper = new SubscriptionWrapper(subscription)
            {
                BeforeOnNext = d => StoreScanCounter.IncCounter1()
            };

            await StoreScanCounter.CaptureAsync(() =>
                _persistence.ReadAllAsync(fromPositionInclusive, wrapper, limit, cancellationToken)
            ).ConfigureAwait(false);
        }

        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            return await ReadLastCounter.CaptureAsync(() =>
                _persistence.ReadLastPositionAsync(cancellationToken)
            ).ConfigureAwait(false);
        }

        public Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId, CancellationToken cancellationToken)
        {
            return PersistCounter.CaptureAsync(() =>
                _persistence.AppendAsync(partitionId, index, payload, operationId, cancellationToken)
            );
        }

        public Task<IChunk> ReplaceOneAsync(
            long position,
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            return ReplaceOneCounter.CaptureAsync(() =>
                _persistence.ReplaceOneAsync(position, partitionId, index, payload, operationId, cancellationToken)
            );
        }

        public Task<IChunk> ReadOneAsync(long position, CancellationToken cancellationToken)
        {
            return ReadOneCounter.CaptureAsync(() =>
                _persistence.ReadOneAsync(position, cancellationToken)
            );
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            await DeleteCounter.CaptureAsync(() =>
                _persistence.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive, cancellationToken)
            ).ConfigureAwait(false);
        }

        public Task<IChunk> ReadByOperationIdAsync(string partitionId, string operationId, CancellationToken cancellationToken)
        {
            return _persistence.ReadByOperationIdAsync(partitionId, operationId, cancellationToken);
        }

        public Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription, CancellationToken cancellationToken)
        {
            return _persistence.ReadAllByOperationIdAsync(operationId, subscription, cancellationToken);
        }
    }
}