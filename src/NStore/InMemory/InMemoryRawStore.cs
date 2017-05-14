using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.InMemory
{
    //@@TODO make concurrency friendly
    public class InMemoryRawStore : IRawStore
    {
        private readonly object _lock = new object();
        private readonly List<Chunk> _chunks = new List<Chunk>();
        private readonly Dictionary<string, Partition> _partitions = new Dictionary<string, Partition>();
        private int _sequence = 0;

        public Task ScanPartitionAsync(
            string partitionId,
            long fromIndexInclusive,
            ScanDirection direction,
            IPartitionObserver partitionObserver,
            long toIndexInclusive = Int64.MaxValue,
            int limit = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            lock (_lock)
            {
                Partition partition;
                if (_partitions.TryGetValue(partitionId, out partition) == false)
                    return Task.FromResult(0);

                IEnumerable<Chunk> list = partition.Chunks.AsEnumerable();

                if (direction == ScanDirection.Backward)
                {
                    list = list.Reverse();
                }

                list = list.Where(x => x.Index >= fromIndexInclusive && x.Index <= toIndexInclusive)
                    .Take(limit);

                foreach (var chunk in list)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (partitionObserver.Observe(chunk.Index, chunk.Payload) == ScanCallbackResult.Stop)
                    {
                        break;
                    }
                }
            }

            return Task.FromResult(0);
        }

        public Task ScanStoreAsync(
            long sequenceStart,
            ScanDirection direction,
            IStoreObserver observer,
            int limit = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            lock (_lock)
            {
                IEnumerable<Chunk> list;
                if (direction == ScanDirection.Forward)
                {
                    list = _chunks.Where(x => x.Id >= sequenceStart)
                        .Take(limit);
                }
                else
                {
                    list = _chunks.ToArray()
                        .Reverse()
                        .Where(x => x.Id <= sequenceStart)
                        .Take(limit);
                }

                foreach (var chunk in list)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (observer.Observe(chunk.Id, chunk.PartitionId, chunk.Index, chunk.Payload) == ScanCallbackResult.Stop)
                    {
                        break;
                    }
                }
            }

            return Task.FromResult(0);
        }

        public Task PersistAsync(
            string partitionId,
            long index,
            object payload,
            string operationId = null,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            lock (_lock)
            {
                var id = ++_sequence;
                var chunk = new Chunk()
                {
                    Id = id,
                    Index = index >= 0 ? index : id,
                    OpId = operationId ?? Guid.NewGuid().ToString(),
                    PartitionId = partitionId,
                    Payload = payload
                };

                Partition partion;
                if (!_partitions.TryGetValue(partitionId, out partion))
                {
                    partion = new Partition(partitionId);
                    _partitions[partitionId] = partion;
                }

                partion.Write(chunk);

                _chunks.Add(chunk);
            }

            return Task.FromResult(0);
        }

        public Task DeleteAsync(
            string partitionId,
            long fromIndex = 0,
            long toIndex = Int64.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            lock (_lock)
            {
                Partition partion;
                if (!_partitions.TryGetValue(partitionId, out partion))
                {
                    throw new StreamDeleteException(partitionId);
                }

                Chunk[] deleted = partion.Delete(fromIndex, toIndex);
                if (deleted.Length == 0)
                {
                    throw new StreamDeleteException(partitionId);
                }

                foreach (var d in deleted)
                {
                    _chunks.Remove(d);
                }
            }

            return Task.FromResult(0);
        }
    }
}