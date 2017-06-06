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
        private readonly List<Chunk> _chunks = new List<Chunk>();

        private readonly ConcurrentDictionary<string, InMemoryPartition> _partitions =
            new ConcurrentDictionary<string, InMemoryPartition>();

        private int _sequence = 0;
        private readonly INetworkSimulator _networkSimulator;
        private readonly InMemoryPartition _emptyInMemoryPartition;

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
            _emptyInMemoryPartition = new InMemoryPartition("::empty", _networkSimulator, Clone);
            _partitions.TryAdd(_emptyInMemoryPartition.Id, _emptyInMemoryPartition);
        }

        public Task ReadPartitionForward(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            InMemoryPartition inMemoryPartition;
            if (!_partitions.TryGetValue(partitionId, out inMemoryPartition))
            {
                return Task.CompletedTask;
            }

            return inMemoryPartition.ReadForward(
                fromLowerIndexInclusive,
                subscription,
                toUpperIndexInclusive,
                limit,
                cancellationToken
            );
        }

        public Task ReadPartitionBackward(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            InMemoryPartition inMemoryPartition;
            if (!_partitions.TryGetValue(partitionId, out inMemoryPartition))
            {
                return Task.CompletedTask;
            }

            return inMemoryPartition.ReadBackward(
                fromUpperIndexInclusive,
                subscription,
                toLowerIndexInclusive,
                limit,
                cancellationToken
            );
        }

        public Task<IChunk> PeekPartition(string partitionId, int maxValue, CancellationToken cancellationToken)
        {
            InMemoryPartition inMemoryPartition;
            if (!_partitions.TryGetValue(partitionId, out inMemoryPartition))
            {
                return Task.FromResult<IChunk>(null);
            }

            return inMemoryPartition.Peek(maxValue, cancellationToken);
        }

        private Chunk Clone(Chunk source)
        {
            if (source == null)
                return null;

            return new Chunk()
            {
                Position = source.Position,
                Index = source.Index,
                OpId = source.OpId,
                PartitionId = source.PartitionId,
                Payload = _cloneFunc(source.Payload)
            };
        }

        public async Task ReadAllAsync(
            long fromSequenceIdInclusive,
            ReadDirection direction,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken
        )
        {
            Chunk[] list;

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

            foreach (var chunk in list)
            {
                await _networkSimulator.Wait().ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                if (!await subscription.OnNext(Clone(chunk)))
                {
                    break;
                }
            }

            await subscription.Completed();
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

            var partion = _partitions.GetOrAdd(partitionId,
                new InMemoryPartition(partitionId, _networkSimulator, Clone)
            );

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