using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
    public class TaskProfilingInfo
    {
        private long _calls;
        private long _exceptions;
        private long _ticks;
        private long _counter1;

        public string Name { get; }

        // fond a bug on macos
        public TimeSpan Elapsed => TimeSpan.FromMilliseconds(_ticks / TimeSpan.TicksPerMillisecond);
        public long Calls => _calls;
        public long Exceptions => _exceptions;
        public long Counter1 => _counter1;
        public string Counter1Name { get; }

        public TaskProfilingInfo(string name, string counter1Name = null)
        {
            Name = name;
            Counter1Name = counter1Name ?? "cnt1";
        }

        public async Task CaptureAsync(Func<Task> task)
        {
            Interlocked.Increment(ref _calls);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            try
            {
                await Task.Delay(100);
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
            await task().ConfigureAwait(false);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0} {1} calls", Name.PadRight(20), _calls);

            if (_exceptions > 0)
            {
                sb.AppendFormat(" ({0} exceptions)", _exceptions);
            }

            sb.AppendFormat(" in {0}ms.", Elapsed.Milliseconds);

            if (_counter1 > 0)
            {
                sb.AppendFormat(" {0} = {1}", Counter1Name, _counter1);
            }

            return sb.ToString();
        }

        public void IncCounter1()
        {
            Interlocked.Increment(ref _counter1);
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
            PartitionScanCounter = new TaskProfilingInfo("Partition scan", "chunks read");
            DeleteCounter = new TaskProfilingInfo("Delete");
            StoreScanCounter = new TaskProfilingInfo("Store Scan");
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
                    partitionObserver,
                    //counter,
                    toIndexInclusive,
                    limit,
                    cancellationToken
                ));
        }

        public async Task ScanStoreAsync(long sequenceStart, ScanDirection direction, IStoreObserver observer,
            int limit = Int32.MaxValue,
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