using System;
using System.Collections.Generic;
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

    public class InMemoryRawStore : IRawStore
    {
        private readonly object _lock = new object();
        private readonly List<Chunk> _chunks = new List<Chunk>();
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
                if (direction == ScanDirection.Forward)
                {
                    var max = limit == Int32.MaxValue ? _chunks.Count : sequenceStart + limit;
                    for (var index = (int) sequenceStart; index < max; index++)
                    {
                        var chunk = _chunks[index];
                        if (chunk.PartitionId == partitionId)
                        {
                            if (consume(chunk.Index, chunk.Payload) == ScanCallbackResult.Stop)
                            {
                                break;
                            }
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
                        if (chunk.PartitionId == partitionId)
                        {
                            if (consume(chunk.Index, chunk.Payload) == ScanCallbackResult.Stop)
                            {
                                break;
                            }
                        }
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
                _chunks.Add(new Chunk()
                {
                    Id = ++_sequence,
                    Index = index,
                    OpId = operationId,
                    PartitionId = partitionId,
                    Payload = payload
                });
            }

            return Task.FromResult(0);
        }

        public Task DeleteAsync(string partitionId, long fromIndex = 0, long toIndex = Int64.MaxValue)
        {
            lock (_lock)
            {
                for (var i = _chunks.Count; i >= 0; i--)
                {
                    if (_chunks[i].PartitionId == partitionId &&
                        _chunks[i].Index >= fromIndex &&
                        _chunks[i].Index <= toIndex
                    )
                    {
                        _chunks.RemoveAt(i);
                    }
                }
            }

            return Task.FromResult(0);
        }
    }
}