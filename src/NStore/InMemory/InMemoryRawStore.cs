using System;
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
        private readonly Partition _emptyPartition = new Partition("::empty");

        public InMemoryRawStore() : this(null, null)
        {
        }

        public InMemoryRawStore(INetworkSimulator networkSimulator)
            : this(networkSimulator, null)
        {
        }

        public InMemoryRawStore(Func<object, object> cloneFunc)
            : this(null, cloneFunc)
        {
        }

        public InMemoryRawStore(INetworkSimulator networkSimulator, Func<object, object> cloneFunc)
        {
            _cloneFunc = cloneFunc ?? (o => o);
            _networkSimulator = networkSimulator ?? new NoNetworkLatencySimulator();
            _partitions.Add(_emptyPartition.Id, _emptyPartition);
        }

        public Task ReadPartitionForward(string partitionId, long fromLowerIndexInclusive, IPartitionConsumer partitionConsumer)
        {
            return ReadPartitionForward(
                partitionId, 
                fromLowerIndexInclusive, 
                partitionConsumer, 
                long.MaxValue,
                int.MaxValue, 
                CancellationToken.None
            );
        }

        public Task ReadPartitionForward(string partitionId, long fromLowerIndexInclusive, IPartitionConsumer partitionConsumer,
            long toUpperIndexInclusive)
        {
            return ReadPartitionForward(
                partitionId, 
                fromLowerIndexInclusive, 
                partitionConsumer, 
                toUpperIndexInclusive,
                int.MaxValue, 
                CancellationToken.None
            );
        }

        public async Task ReadPartitionForward(
            string partitionId,
            long fromLowerIndexInclusive,
            IPartitionConsumer partitionConsumer,
            long toUpperIndexInclusive ,
            int limit,
            CancellationToken cancellationToken
        )
        {
            Chunk[] result;
            lock (_lock)
            {
                Partition partition;
                if (!_partitions.TryGetValue(partitionId, out partition))
                {
                    return;
                }

                var list = partition.Chunks.AsEnumerable();

                result = list.Where(x => x.Index >= fromLowerIndexInclusive && x.Index <= toUpperIndexInclusive)
                    .Take(limit)
                    .ToArray();
            }

            foreach (var chunk in result)
            {
                await _networkSimulator.WaitFast().ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                if (partitionConsumer.Consume(chunk.Index, _cloneFunc(chunk.Payload)) == ScanAction.Stop)
                {
                    break;
                }
            }
        }

        public async Task ReadPartitionBackward(
            string partitionId,
            long fromUpperIndexInclusive,
            IPartitionConsumer partitionConsumer,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            Chunk[] result;
            lock (_lock)
            {
                Partition partition;
                if (!_partitions.TryGetValue(partitionId, out partition))
                {
                    return;
                }

                result = partition.Chunks.Reverse()
                    .Where(x => x.Index <= fromUpperIndexInclusive && x.Index >= toLowerIndexInclusive)
                    .Take(limit)
                    .ToArray();
            }

            foreach (var chunk in result)
            {
                await _networkSimulator.WaitFast().ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                if (partitionConsumer.Consume(chunk.Index, _cloneFunc(chunk.Payload)) == ScanAction.Stop)
                {
                    break;
                }
            }
        }

        public async Task ScanStoreAsync(
            long sequenceStart,
            ScanDirection direction,
            IStoreConsumer consumer,
            int limit ,
            CancellationToken cancellationToken 
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
                    list = _chunks
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
                if (await consumer.Consume(chunk.Id, chunk.PartitionId, chunk.Index, _cloneFunc(chunk.Payload)) ==
                    ScanAction.Stop)
                {
                    break;
                }
            }
        }

        public async Task PersistAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken
        )
        {
            var id = Interlocked.Increment(ref _sequence);
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
                    // keep same id to avoid holes in the stream
                    chunk.PartitionId = "::empty";
                    chunk.Index = chunk.Id;
                    chunk.OpId = chunk.Id.ToString();
                    chunk.Payload = null;
                    _emptyPartition.Write(chunk);
                    _chunks.Add(chunk);
                    throw;
                }
                _chunks.Add(chunk);
            }
            await _networkSimulator.Wait().ConfigureAwait(false);
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromIndex ,
            long toIndex ,
            CancellationToken cancellationToken
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

                var deleted = partition.Delete(fromIndex, toIndex);
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