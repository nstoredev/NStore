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
        public TaskProfilingInfo PartitionScanCounter { get; }

        public ProfileDecorator(IRawStore store)
        {
            _store = store;
            PersistCounter = new TaskProfilingInfo("Persist");
            PartitionScanCounter = new TaskProfilingInfo("Partition scan", "chunks read");
            DeleteCounter = new TaskProfilingInfo("Delete");
            StoreScanCounter = new TaskProfilingInfo("Store Scan", "chunks read");
        }

        public async Task ScanPartitionAsync(string partitionId, long fromIndexInclusive, ScanDirection direction,
            IPartitionObserver partitionObserver, long toIndexInclusive = Int64.MaxValue, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var counter = new LambdaPartitionObserver((l, o) =>
            {
                PartitionScanCounter.IncCounter1();
                return partitionObserver.Observe(l, o);
            });

            await PartitionScanCounter.CaptureAsync(() =>
                _store.ScanPartitionAsync(
                    partitionId,
                    fromIndexInclusive,
                    direction,
                    counter,
                    toIndexInclusive,
                    limit,
                    cancellationToken
                ));
        }

        public async Task ScanStoreAsync(long sequenceStart, ScanDirection direction, IStoreObserver observer,
            int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
			var storeObserver = new LambdaStoreObserver((si,s, l, o) =>
            {
            	StoreScanCounter.IncCounter1();
            	return observer.Observe(si,s,l, o);
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