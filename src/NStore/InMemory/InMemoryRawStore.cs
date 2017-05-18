using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.InMemory
{
    public class InMemoryRawStore : IRawStore
    {
        private readonly Func<object, object> _cloneFunc;
        private readonly object _lock = new object();
        private readonly List<Chunk> _chunks = new List<Chunk>();
        private readonly Dictionary<string, Partition> _partitions = new Dictionary<string, Partition>();
        private int _sequence = 0;
        private readonly INetworkSimulator _networkSimulator;

        public InMemoryRawStore(INetworkSimulator networkSimulator = null, Func<object, object> cloneFunc = null)
        {
            _cloneFunc = cloneFunc ?? (o => o);
            _networkSimulator = networkSimulator ?? new LocalhostSimulator();
        }

        public async Task ScanPartitionAsync(
            string partitionId,
            long fromIndexInclusive,
            ScanDirection direction,
            IPartitionObserver partitionObserver,
            long toIndexInclusive = Int64.MaxValue,
            int limit = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            Chunk[] result;
            lock (_lock)
            {
                Partition partition;
                if (_partitions.TryGetValue(partitionId, out partition) == false)
                {
                    return;
                }

                IEnumerable<Chunk> list = partition.Chunks.AsEnumerable();

                if (direction == ScanDirection.Backward)
                {
                    list = list.Reverse();
                }

                result = list.Where(x => x.Index >= fromIndexInclusive && x.Index <= toIndexInclusive)
                    .Take(limit)
                    .ToArray();
            }

            foreach (var chunk in result)
            {
                await _networkSimulator.WaitFast().ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                if (partitionObserver.Observe(chunk.Index, _cloneFunc(chunk.Payload)) == ScanCallbackResult.Stop)
                {
                    break;
                }
            }
        }

        public async Task ScanStoreAsync(
            long sequenceStart,
            ScanDirection direction,
            IStoreObserver observer,
            int limit = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            Chunk[] list;

            lock (_lock)
            {
                if (direction == ScanDirection.Forward)
                {
                    list = _chunks.Where(x => x.Id >= sequenceStart)
                        .OrderBy(x => x.Id)
                        .Take(limit)
                        .ToArray();
                }
                else
                {
                    list = _chunks.ToArray()
                        .Where(x => x.Id <= sequenceStart)
                        .OrderByDescending(x => x.Id)
                        .Take(limit)
                        .ToArray();
                }
            }

            foreach (var chunk in list)
            {
                await _networkSimulator.Wait().ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
                if (observer.Observe(chunk.Id, chunk.PartitionId, chunk.Index, _cloneFunc(chunk.Payload)) == ScanCallbackResult.Stop)
                {
                    break;
                }
            }
        }

        public async Task PersistAsync(
            string partitionId,
            long index,
            object payload,
            string operationId = null,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            var id = ++_sequence;
            var chunk = new Chunk()
            {
                Id = id,
                Index = index >= 0 ? index : id,
                OpId = operationId ?? Guid.NewGuid().ToString(),
                PartitionId = partitionId,
                Payload = _cloneFunc(payload)
            };

            await _networkSimulator.Wait().ConfigureAwait(false);

            lock (_lock)
            {
                Partition partion;
                if (!_partitions.TryGetValue(partitionId, out partion))
                {
                    partion = new Partition(partitionId);
                    _partitions[partitionId] = partion;
                }

                try
                {
                    partion.Write(chunk);
                }
                catch (DuplicateStreamIndexException)
                {
                    // write empty chunk
                    chunk.PartitionId = "::system";
                    chunk.Index = chunk.Id;
                    chunk.OpId = chunk.Id.ToString();
                    _chunks.Add(chunk);
                    throw;
                }
                _chunks.Add(chunk);
            }
            await _networkSimulator.Wait().ConfigureAwait(false);
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromIndex = 0,
            long toIndex = Int64.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            await _networkSimulator.Wait().ConfigureAwait(false);
            lock (_lock)
            {
                Partition partition;
                if (!_partitions.TryGetValue(partitionId, out partition))
                {
                    throw new StreamDeleteException(partitionId);
                }

                Chunk[] deleted = partition.Delete(fromIndex, toIndex);
                if (deleted.Length == 0)
                {
                    throw new StreamDeleteException(partitionId);
                }

                foreach (var d in deleted)
                {
                    _chunks.Remove(d);
                }
            }
        }
    }
}