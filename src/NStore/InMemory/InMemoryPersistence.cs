using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;

namespace NStore.InMemory
{
    public class InMemoryPersistence : IPersistence
    {
        private readonly Func<object, object> _cloneFunc;
        private readonly object _lock = new object();
        private readonly List<Chunk> _chunks = new List<Chunk>();
        private readonly ConcurrentDictionary<string, InMemoryPartition> _partitions = new ConcurrentDictionary<string, InMemoryPartition>();
        private int _sequence = 0;
        private readonly INetworkSimulator _networkSimulator;
        private readonly InMemoryPartition _emptyInMemoryPartition = new InMemoryPartition("::empty");

        public InMemoryPersistence() : this(null, null)
        {
        }

        public InMemoryPersistence(INetworkSimulator networkSimulator)
            : this(networkSimulator, null)
        {
        }

        public InMemoryPersistence(Func<object, object> cloneFunc)
            : this(null, cloneFunc)
        {
        }

        public InMemoryPersistence(INetworkSimulator networkSimulator, Func<object, object> cloneFunc)
        {
            _cloneFunc = cloneFunc ?? (o => o);
            _networkSimulator = networkSimulator ?? new NoNetworkLatencySimulator();
            _partitions.TryAdd(_emptyInMemoryPartition.Id, _emptyInMemoryPartition);
        }

        public async Task ReadPartitionForward(
            string partitionId,
            long fromLowerIndexInclusive,
            IPartitionConsumer partitionConsumer,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            Chunk[] result;
            lock (_lock)
            {
                InMemoryPartition inMemoryPartition;
                if (!_partitions.TryGetValue(partitionId, out inMemoryPartition))
                {
                    return;
                }

                result = inMemoryPartition.Chunks
                    .Where(x => x.Index >= fromLowerIndexInclusive && x.Index <= toUpperIndexInclusive)
                    .Take(limit)
                    .ToArray();
            }

            await StartProducer(partitionConsumer, result, cancellationToken);
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
                InMemoryPartition inMemoryPartition;
                if (!_partitions.TryGetValue(partitionId, out inMemoryPartition))
                {
                    return;
                }

                result = inMemoryPartition.Chunks.Reverse()
                    .Where(x => x.Index <= fromUpperIndexInclusive && x.Index >= toLowerIndexInclusive)
                    .Take(limit)
                    .ToArray();
            }

            await StartProducer(partitionConsumer, result, cancellationToken);
        }

        public Task<IPartitionData> PeekPartition(string partitionId, int maxValue, CancellationToken cancellationToken)
        {
            InMemoryPartition inMemoryPartition;
            lock (_lock)
            {
                if (!_partitions.TryGetValue(partitionId, out inMemoryPartition))
                {
                    return Task.FromResult<IPartitionData>(null);
                }
            }

            var chunk = inMemoryPartition.Chunks.Reverse()
                    .Where(x => x.Index <= maxValue)
                    .Take(1)
                    .SingleOrDefault();

            if (chunk != null)
            {
                chunk.Payload = _cloneFunc(chunk.Payload);
            }

            return Task.FromResult<IPartitionData>(chunk);
        }

        private async Task StartProducer(
            IPartitionConsumer partitionConsumer,
            IEnumerable<Chunk> chunks,
            CancellationToken cancellationToken)
        {
            try
            {
                foreach (var chunk in chunks)
                {
                    await _networkSimulator.WaitFast().ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();

                    chunk.Payload = _cloneFunc(chunk.Payload);

                    if (!await partitionConsumer.OnNext(chunk))
                    {
                        await partitionConsumer.Completed();
                        return;
                    }
                }
            }
            catch (Exception e)
            {
                await partitionConsumer.OnError(e);
                return;
            }

            await partitionConsumer.Completed();
        }

        public async Task ReadAllAsync(
            long fromSequenceIdInclusive,
            ReadDirection direction,
            IAllPartitionsConsumer consumer,
            int limit,
            CancellationToken cancellationToken
        )
        {
            Chunk[] list;

            lock (_lock)
            {
                if (direction == ReadDirection.Forward)
                {
                    list = _chunks.Where(x => x.Position >= fromSequenceIdInclusive)
                        .OrderBy(x => x.Position)
                        .Take(limit)
                        .ToArray();
                }
                else
                {
                    list = _chunks
                        .Where(x => x.Position <= fromSequenceIdInclusive)
                        .OrderByDescending(x => x.Position)
                        .Take(limit)
                        .ToArray();
                }
            }

            foreach (var chunk in list)
            {
                await _networkSimulator.Wait().ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
                if (await consumer.Consume(chunk.Position, chunk.PartitionId, chunk.Index, _cloneFunc(chunk.Payload)) ==
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
                Position = id,
                Index = index >= 0 ? index : id,
                OpId = operationId ?? Guid.NewGuid().ToString(),
                PartitionId = partitionId,
                Payload = _cloneFunc(payload)
            };

            await _networkSimulator.Wait().ConfigureAwait(false);

            lock (_lock)
            {
                InMemoryPartition partion;
                if (!_partitions.TryGetValue(partitionId, out partion))
                {
                    partion = new InMemoryPartition(partitionId);
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
                    chunk.Index = chunk.Position;
                    chunk.OpId = chunk.Position.ToString();
                    chunk.Payload = null;
                    _emptyInMemoryPartition.Write(chunk);
                    _chunks.Add(chunk);
                    throw;
                }
                _chunks.Add(chunk);
            }
            await _networkSimulator.Wait().ConfigureAwait(false);
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken
        )
        {
            await _networkSimulator.Wait().ConfigureAwait(false);
            lock (_lock)
            {
                InMemoryPartition inMemoryPartition;
                if (!_partitions.TryGetValue(partitionId, out inMemoryPartition))
                {
                    throw new StreamDeleteException(partitionId);
                }

                var deleted = inMemoryPartition.Delete(fromLowerIndexInclusive, toUpperIndexInclusive);
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