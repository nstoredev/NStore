using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;
using NStore.Reactive;

namespace NStore.InMemory
{
    public sealed class MemoryPartition :
        IPublisher<IPartitionData>,
        ISubscription
    {
        private readonly ConcurrentQueue<long> _requests = new ConcurrentQueue<long>();
        private ISubscriber<IPartitionData> _subscriber;
        public void Subscribe(ISubscriber<IPartitionData> subscriber)
        {
            // throw if already subscribed
            _subscriber = subscriber;
            subscriber.OnSubscribe(this);
            Run();
        }

        public void Request(long n)
        {
            _requests.Enqueue(n);
        }

        public void Cancel()
        {
        }

        private void Run()
        {
            while (_requests.TryDequeue(out long batch))
            {
                // query
                
                // scan
                
                // signal
                
            
            }
        }
    }

    public class MemorySubscriber : ISubscriber<IPartitionData>
    {
        private ISubscription _subscription;

        public void OnSubscribe(ISubscription subscription)
        {
            _subscription = subscription;
            _subscription.Request(1);
        }

        public void OnNext(IPartitionData element)
        {
            _subscription.Request(1);
        }

        public void OnError(Exception cause)
        {
            // if transient error => renew subscription?
            throw new NotImplementedException();
        }

        public void OnComplete()
        {
            throw new NotImplementedException();
        }
    }


    public class InMemoryPersistence : IPersistence
    {
        private readonly Func<object, object> _cloneFunc;
        private readonly object _lock = new object();
        private readonly List<Chunk> _chunks = new List<Chunk>();
        private readonly Dictionary<string, Partition> _partitions = new Dictionary<string, Partition>();
        private int _sequence = 0;
        private readonly INetworkSimulator _networkSimulator;
        private readonly Partition _emptyPartition = new Partition("::empty");

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
            _partitions.Add(_emptyPartition.Id, _emptyPartition);
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

            await StartProducer(partitionConsumer, cancellationToken, result);
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

            await StartProducer(partitionConsumer, cancellationToken, result);
        }

        private async Task StartProducer(IPartitionConsumer partitionConsumer, CancellationToken cancellationToken,
            Chunk[] result)
        {
            try
            {
                foreach (var chunk in result)
                {
                    await _networkSimulator.WaitFast().ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();

                    if (partitionConsumer.Consume(chunk.Index, _cloneFunc(chunk.Payload)) == ScanAction.Stop)
                    {
                        partitionConsumer.Completed();
                        return;
                    }
                }
            }
            catch (Exception e)
            {
                partitionConsumer.OnError(e);
                return;
            }

            partitionConsumer.Completed();
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
                    chunk.Index = chunk.Position;
                    chunk.OpId = chunk.Position.ToString();
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
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
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

                var deleted = partition.Delete(fromLowerIndexInclusive, toUpperIndexInclusive);
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