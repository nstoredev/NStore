using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
    public class TaskProfilingInfo
    {
        private long _calls;
        private long _exceptions;
        private long _ticks;
        public string Name { get; }

        public TimeSpan Elapsed => TimeSpan.FromTicks(_ticks);
        public long Calls => _calls;
        public long Exceptions => _exceptions;

        public TaskProfilingInfo(string name)
        {
            Name = name;
        }

        public async Task CaptureAsync(Func<Task> task)
        {
            Interlocked.Increment(ref _calls);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            try
            {
                await task();
            }
            catch (Exception)
            {
                Interlocked.Increment(ref _exceptions);
            }
            finally
            {
                sw.Stop();
                Interlocked.Add(ref _ticks, sw.ElapsedTicks);
            }
        }

        public override string ToString()
        {
            return $"{Name.PadRight(20)} : {_calls} calls ({_exceptions} exceptions) in {Elapsed.TotalMilliseconds} ms";
        }
    }

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
            PartitionScanCounter = new TaskProfilingInfo("Partition scan");
            DeleteCounter = new TaskProfilingInfo("Delete");
            StoreScanCounter = new TaskProfilingInfo("Store Scan");
        }

        public async Task ScanPartitionAsync(string partitionId, long fromIndexInclusive, ScanDirection direction,
            IPartitionObserver partitionObserver, long toIndexInclusive = Int64.MaxValue, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            await PartitionScanCounter.CaptureAsync(() =>
                _store.ScanPartitionAsync(partitionId, fromIndexInclusive, direction, partitionObserver,
                    toIndexInclusive, limit, cancellationToken)
            );
        }

        public async Task ScanStoreAsync(long sequenceStart, ScanDirection direction, IStoreObserver observer, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            await StoreScanCounter.CaptureAsync(() =>
                _store.ScanStoreAsync(sequenceStart, direction, observer, limit, cancellationToken)
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