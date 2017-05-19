using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
    public class StatsDecorator : IRawStore
    {
        private readonly IRawStore _store;
        private int _persistCount;
		private int _deleteCount;
		private int _scanStoreCalls;
		private int _scanPartitionCalls;

        public StatsDecorator(IRawStore store)
        {
            _store = store;
        }

        public int TotalPersists => _persistCount;
        public int TotalDeletes => _deleteCount;
		public int ScanStoreCalls => _scanStoreCalls;
		public int ScanPartitionCalls => _scanPartitionCalls;

        public async Task ScanPartitionAsync(string partitionId, long fromIndexInclusive, ScanDirection direction,
            IPartitionObserver partitionObserver, long toIndexInclusive = Int64.MaxValue, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
			Interlocked.Increment(ref _scanPartitionCalls);
            await _store.ScanPartitionAsync(partitionId, fromIndexInclusive, direction, partitionObserver, toIndexInclusive, limit, cancellationToken);
		}

        public async Task ScanStoreAsync(long sequenceStart, ScanDirection direction, IStoreObserver observer, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
			Interlocked.Increment(ref _scanStoreCalls);
            await _store.ScanStoreAsync(sequenceStart, direction, observer, limit, cancellationToken);
        }

        public async Task PersistAsync(string partitionId, long index, object payload, string operationId = null,
            CancellationToken cancellationToken = new CancellationToken())
        {
            Interlocked.Increment(ref _persistCount);
            await _store.PersistAsync(partitionId, index, payload, operationId, cancellationToken);
        }

        public async Task DeleteAsync(string partitionId, long fromIndex = 0, long toIndex = Int64.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
			Interlocked.Increment(ref _deleteCount);
			await _store.DeleteAsync(partitionId, fromIndex, toIndex, cancellationToken);
        }
    }
}