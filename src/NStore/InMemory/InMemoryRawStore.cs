using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.InMemory
{
    internal class Chunk
    {
        public long Id { get; set; }
        public string PartitionId { get; set; }
        public long Index { get; set; }
        public object Payload { get; set; }
        public string OpId { get; set; }
    }

    internal class Partition
    {
        public Partition(string partitionId)
        {
            this.Id = partitionId;
        }

        public string Id { get; set; }
        public IEnumerable<Chunk> Chunks => _byIndex.Values.OrderBy(x => x.Index);
        private readonly IDictionary<long, Chunk> _byIndex = new Dictionary<long, Chunk>();

        public void Write(Chunk chunk)
        {
            _byIndex.Add(chunk.Index, chunk);
        }

        public Chunk[] Delete(long fromIndex, long toIndex)
        {
            var toDelete = Chunks.Where(x => x.Index >= fromIndex && x.Index <= toIndex).ToArray();

            foreach (var chunk in toDelete)
            {
                this._byIndex.Remove(chunk.Index);
            }

            return toDelete;
        }
    }

    //@@TODO make concurrency friendly
    public class InMemoryRawStore : IRawStore
    {
        private readonly object _lock = new object();
        private readonly List<Chunk> _chunks = new List<Chunk>();
        private readonly Dictionary<string, Partition> _partitions = new Dictionary<string, Partition>();
        private int _sequence = 0;

        public Task ScanAsync(
            string partitionId,
            long sequenceStart,
            ScanDirection direction,
            Func<long, object, ScanCallbackResult> consume,
            int limit = Int32.MaxValue)
        {
            lock (_lock)
            {
                Partition partition;
                if (_partitions.TryGetValue(partitionId, out partition) == false)
                    return Task.FromResult(0);

                IEnumerable<Chunk> list = null;

                if (direction == ScanDirection.Forward)
                {
                    list = partition.Chunks
                        .Where(x => x.Index >= sequenceStart)
                        .Take(limit);
                }
                else
                {
                    list = partition.Chunks
                        .Reverse()
                        .Where(x => x.Index <= sequenceStart)
                        .Take(limit);
                }

                foreach (var chunk in list)
                {
                    if (consume(chunk.Index, chunk.Payload) == ScanCallbackResult.Stop)
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
            Func<long, object, ScanCallbackResult> consume,
            int limit = Int32.MaxValue
        )
        {
            lock (_lock)
            {
                if (direction == ScanDirection.Forward)
                {
                    var max = Math.Min(_chunks.Count, limit == Int32.MaxValue ? _chunks.Count : sequenceStart + limit);
                    for (var index = (int) sequenceStart; index < max; index++)
                    {
                        var chunk = _chunks[index];
                        if (consume(chunk.Index, chunk.Payload) == ScanCallbackResult.Stop)
                        {
                            break;
                        }
                    }
                }
                else
                {
                    if (sequenceStart == Int64.MaxValue)
                        sequenceStart = _chunks.Count - 1;

                    var max = Math.Max(0, limit == Int32.MaxValue ? 0 : sequenceStart - limit);

                    for (var index = (int) sequenceStart; index >= max; index--)
                    {
                        var chunk = _chunks[index];
                        if (consume(chunk.Index, chunk.Payload) == ScanCallbackResult.Stop)
                        {
                            break;
                        }
                    }
                }
            }

            return Task.FromResult(0);
        }

        public Task PersistAsync(string partitionId, long index, object payload, string operationId = null)
        {
            lock (_lock)
            {
                var id = ++_sequence;
                var chunk = new Chunk()
                {
                    Id = id,
                    Index = index >= 0 ? index : id,
                    OpId = operationId,
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

        public Task DeleteAsync(string partitionId, long fromIndex = 0, long toIndex = Int64.MaxValue)
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