using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Core.InMemory
{
    public class InMemoryPersistence : IPersistence, IEnhancedPersistence, IDisposable
    {
        private bool _disposed;
        private readonly Func<object, object> _cloneFunc;
        private readonly MemoryChunk[] _chunks;

        private readonly ConcurrentDictionary<string, InMemoryPartition> _partitions =
            new ConcurrentDictionary<string, InMemoryPartition>();

        private int _sequence = 0;
        private int _lastWrittenPosition = -1;
        private readonly INetworkSimulator _networkSimulator;
        private readonly InMemoryPartition _emptyInMemoryPartition;
        private readonly ReaderWriterLockSlim _lockSlim = new ReaderWriterLockSlim();
        private readonly InMemoryPersistenceOptions _options;
        private const string EmptyPartitionId = "::empty";

        public bool SupportsFillers => true;

        public InMemoryPersistence() : this(new InMemoryPersistenceOptions())
        {
        }

        public InMemoryPersistence(INetworkSimulator networkSimulator)
            : this(new InMemoryPersistenceOptions(null, networkSimulator))
        {
        }

        public InMemoryPersistence(Func<object, object> cloneFunc)
            : this(new InMemoryPersistenceOptions(cloneFunc, null))
        {
        }

        /// <summary>
        /// Use only for debug / test
        /// </summary>
        public IEnumerable<string> PartitionIds => _partitions.Keys.Where(x => x != EmptyPartitionId);

        public InMemoryPersistence(InMemoryPersistenceOptions options)
        {
            _chunks = new MemoryChunk[1024 * 1024];
            _options = options;
            _cloneFunc = _options.CloneFunc ?? (o => o);
            _networkSimulator = _options.NetworkSimulator ?? new NoNetworkLatencySimulator();
            _emptyInMemoryPartition = new InMemoryPartition("::empty", _networkSimulator, Clone);
            _partitions.TryAdd(_emptyInMemoryPartition.Id, _emptyInMemoryPartition);
        }

        public Task ReadForwardAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            if (partitionId == null)
            {
                throw new ArgumentNullException(nameof(partitionId));
            }

            if (!_partitions.TryGetValue(partitionId, out var partition))
            {
                return Task.CompletedTask;
            }

            return partition.ReadForward(
                fromLowerIndexInclusive,
                subscription,
                toUpperIndexInclusive,
                limit,
                cancellationToken
            );
        }

        public async Task ReadForwardMultiplePartitionsAsync(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            if (partitionIdsList is null)
            {
                throw new ArgumentNullException(nameof(partitionIdsList));
            }

            if (!partitionIdsList.Any())
            {
                return;
            }

            // O(m) lookup using dictionary TryGetValue instead of O(n*m) with nested Any()
            var partitions = new List<InMemoryPartition>();
            foreach (var partitionId in partitionIdsList)
            {
                if (_partitions.TryGetValue(partitionId, out var partition))
                {
                    partitions.Add(partition);
                }
            }

            foreach (var partition in partitions)
            {
                await partition.ReadForward(fromLowerIndexInclusive, subscription, toUpperIndexInclusive, Int32.MaxValue, cancellationToken);
            }
        }

#if NET8_0_OR_GREATER
        public async IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsAsyncEnumerable(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (partitionIdsList is null)
            {
                throw new ArgumentNullException(nameof(partitionIdsList));
            }

            // O(m) lookup using dictionary TryGetValue instead of O(n*m) with nested Any()
            var partitions = new List<InMemoryPartition>();
            foreach (var partitionId in partitionIdsList)
            {
                if (_partitions.TryGetValue(partitionId, out var partition))
                {
                    partitions.Add(partition);
                }
            }

            if (partitions.Count == 0)
            {
                yield break;
            }

            foreach (var partition in partitions)
            {
                var recorder = new Recorder();
                await partition.ReadForward(fromLowerIndexInclusive, recorder, toUpperIndexInclusive, Int32.MaxValue, cancellationToken).ConfigureAwait(false);

                foreach (var chunk in recorder.Chunks)
                {
                    yield return chunk;
                }
            }
        }
#endif

        public async Task ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            if (partitionRequests is null)
                throw new ArgumentNullException(nameof(partitionRequests));

            var requestsList = partitionRequests.ToList();
            if (!requestsList.Any())
                return;

            // Group requests by partition id and preserve the order of ranges for each partition.
            var grouped = requestsList.GroupBy(r => r.PartitionId);

            foreach (var group in grouped)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var partitionId = group.Key;
                if (!_partitions.TryGetValue(partitionId, out var partition))
                {
                    continue; // ignore non existing partitions
                }

                // Keep track of seen indices for this partition to avoid duplicates across ranges.
                var seen = new HashSet<long>();
                var wrapper = new PartitionSubscriptionWrapper(subscription, seen);

                // Sort ranges by starting index to ensure per-partition ordering.
                var ranges = group.Select(r => (from: r.FromPartitionIndexInclusive, to: r.ToPartitionIndexInclusive))
                                  .OrderBy(t => t.from)
                                  .ToList();

                foreach (var (from, to) in ranges)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    await partition.ReadForward(from, wrapper, to, Int32.MaxValue, cancellationToken).ConfigureAwait(false);

                    if (wrapper.ShouldStop)
                    {
                        // upstream indicated to stop (OnNextAsync returned false). Stop processing further ranges/partitions.
                        return;
                    }
                }
            }
        }

        public async IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (partitionRequests is null)
                throw new ArgumentNullException(nameof(partitionRequests));

            var requestsList = partitionRequests.ToList();
            if (!requestsList.Any())
                yield break;

            var grouped = requestsList.GroupBy(r => r.PartitionId);

            foreach (var group in grouped)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var partitionId = group.Key;
                if (!_partitions.TryGetValue(partitionId, out var partition))
                {
                    continue; // no such partition
                }

                // Track seen indices per partition to avoid duplicates across ranges.
                var seen = new HashSet<long>();

                var ranges = group.Select(r => (from: r.FromPartitionIndexInclusive, to: r.ToPartitionIndexInclusive))
                                  .OrderBy(t => t.from)
                                  .ToList();

                foreach (var (from, to) in ranges)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // Use a recorder to collect chunks for this range and then yield them.
                    var recorder = new Recorder();
                    await partition.ReadForward(from, recorder, to, Int32.MaxValue, cancellationToken).ConfigureAwait(false);

                    foreach (var chunk in recorder.Chunks)
                    {
                        if (seen.Add(chunk.Index))
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            yield return Clone((MemoryChunk)chunk);
                        }
                    }
                }
            }
        }

        public async Task ReadManyBackwardAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            if (partitionRequests is null)
                throw new ArgumentNullException(nameof(partitionRequests));

            var requestsList = partitionRequests.ToList();
            if (!requestsList.Any())
                return;

            // Group requests by partition id and preserve the order of ranges for each partition.
            var grouped = requestsList.GroupBy(r => r.PartitionId);

            foreach (var group in grouped)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var partitionId = group.Key;
                if (!_partitions.TryGetValue(partitionId, out var partition))
                {
                    continue; // ignore non existing partitions
                }

                // Keep track of seen indices for this partition to avoid duplicates across ranges.
                var seen = new HashSet<long>();
                var wrapper = new PartitionSubscriptionWrapper(subscription, seen);

                // Sort ranges by upper bound descending for backward reading.
                var ranges = group.Select(r => (from: r.FromPartitionIndexInclusive, to: r.ToPartitionIndexInclusive))
                                  .OrderByDescending(t => t.to)
                                  .ToList();

                foreach (var (from, to) in ranges)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    await partition.ReadBackward(to, wrapper, from, Int32.MaxValue, cancellationToken).ConfigureAwait(false);

                    if (wrapper.ShouldStop)
                    {
                        // upstream indicated to stop (OnNextAsync returned false). Stop processing further ranges/partitions.
                        return;
                    }
                }
            }
        }

        public async IAsyncEnumerable<IChunk> ReadManyBackwardAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (partitionRequests is null)
                throw new ArgumentNullException(nameof(partitionRequests));

            var requestsList = partitionRequests.ToList();
            if (!requestsList.Any())
                yield break;

            var grouped = requestsList.GroupBy(r => r.PartitionId);

            foreach (var group in grouped)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var partitionId = group.Key;
                if (!_partitions.TryGetValue(partitionId, out var partition))
                {
                    continue; // no such partition
                }

                // Track seen indices per partition to avoid duplicates across ranges.
                var seen = new HashSet<long>();

                // Sort ranges by upper bound descending for backward reading.
                var ranges = group.Select(r => (from: r.FromPartitionIndexInclusive, to: r.ToPartitionIndexInclusive))
                                  .OrderByDescending(t => t.to)
                                  .ToList();

                foreach (var (from, to) in ranges)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // Use a recorder to collect chunks for this range and then yield them.
                    var recorder = new Recorder();
                    await partition.ReadBackward(to, recorder, from, Int32.MaxValue, cancellationToken).ConfigureAwait(false);

                    foreach (var chunk in recorder.Chunks)
                    {
                        if (seen.Add(chunk.Index))
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            yield return Clone((MemoryChunk)chunk);
                        }
                    }
                }
            }
        }

        public Task<IReadOnlyDictionary<string, IChunk>> ReadLastChunkForPartitionsAsync(
            IEnumerable<string> partitionIds,
            CancellationToken cancellationToken)
        {
            if (partitionIds is null)
                throw new ArgumentNullException(nameof(partitionIds));

            var result = new Dictionary<string, IChunk>();

            foreach (var partitionId in partitionIds)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (string.IsNullOrWhiteSpace(partitionId))
                    continue;

                if (_partitions.TryGetValue(partitionId, out var partition))
                {
                    var lastChunk = partition.GetLastChunk();
                    if (lastChunk != null)
                    {
                        result[partitionId] = Clone(lastChunk);
                    }
                }
            }

            return Task.FromResult<IReadOnlyDictionary<string, IChunk>>(result);
        }

        private class PartitionSubscriptionWrapper : ISubscription
        {
            private readonly ISubscription _inner;
            private readonly HashSet<long> _seen;

            public bool ShouldStop { get; private set; }

            public PartitionSubscriptionWrapper(ISubscription inner, HashSet<long> seen)
            {
                _inner = inner;
                _seen = seen;
            }

            public Task OnStartAsync(long indexOrPosition)
            {
                return _inner.OnStartAsync(indexOrPosition);
            }

            public async Task<bool> OnNextAsync(IChunk chunk)
            {
                if (!_seen.Add(chunk.Index))
                {
                    // already seen this index for this partition -> skip but keep reading
                    return true;
                }

                var cont = await _inner.OnNextAsync(chunk).ConfigureAwait(false);
                if (!cont)
                {
                    ShouldStop = true;
                }

                return cont;
            }

            public Task CompletedAsync(long indexOrPosition)
            {
                return _inner.CompletedAsync(indexOrPosition);
            }

            public Task StoppedAsync(long indexOrPosition)
            {
                return _inner.StoppedAsync(indexOrPosition);
            }

            public Task OnErrorAsync(long indexOrPosition, Exception ex)
            {
                return _inner.OnErrorAsync(indexOrPosition, ex);
            }
        }

        public Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            if (partitionId == null)
            {
                throw new ArgumentNullException(nameof(partitionId));
            }

            if (!_partitions.TryGetValue(partitionId, out var partition))
            {
                return Task.CompletedTask;
            }

            return partition.ReadBackward(
                fromUpperIndexInclusive,
                subscription,
                toLowerIndexInclusive,
                limit,
                cancellationToken
            );
        }

        public Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            if (partitionId == null)
            {
                throw new ArgumentNullException(nameof(partitionId));
            }

            if (!_partitions.TryGetValue(partitionId, out var partition))
            {
                return Task.FromResult<IChunk>(null);
            }

            return partition.Peek(fromUpperIndexInclusive, cancellationToken);
        }

        private MemoryChunk Clone(MemoryChunk source)
        {
            if (source == null)
                return null;

            return new MemoryChunk()
            {
                Position = source.Position,
                Index = source.Index,
                OperationId = source.OperationId,
                PartitionId = source.PartitionId,
                Payload = _cloneFunc(source.Payload)
            };
        }

        public async Task ReadAllAsync(long fromPositionInclusive, ISubscription subscription, int limit,
            CancellationToken cancellationToken)
        {
            await subscription.OnStartAsync(fromPositionInclusive).ConfigureAwait(false);

            int start = (int)Math.Max(fromPositionInclusive - 1, 0);

            _lockSlim.EnterReadLock();
            int lastWritten = _lastWrittenPosition;
            _lockSlim.ExitReadLock();

            if (start > lastWritten)
            {
                await subscription.StoppedAsync(fromPositionInclusive).ConfigureAwait(false);
                return;
            }

            var toRead = Math.Min(limit, lastWritten - start + 1);
            if (toRead <= 0)
            {
                await subscription.StoppedAsync(fromPositionInclusive).ConfigureAwait(false);
                return;
            }

            IEnumerable<MemoryChunk> list = new ArraySegment<MemoryChunk>(_chunks, start, toRead);

            long position = 0;

            try
            {
                foreach (var chunk in list)
                {
                    if (chunk.Deleted)
                    {
                        continue;
                    }

                    position = chunk.Position;

                    await _networkSimulator.Wait().ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();

                    if (!await subscription.OnNextAsync(Clone(chunk)).ConfigureAwait(false))
                    {
                        await subscription.StoppedAsync(position).ConfigureAwait(false);
                        return;
                    }
                }

                if (position == 0)
                {
                    await subscription.StoppedAsync(fromPositionInclusive).ConfigureAwait(false);
                }
                else
                {
                    await subscription.CompletedAsync(position).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                await subscription.StoppedAsync(position).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await subscription.OnErrorAsync(position, e).ConfigureAwait(false);
            }
        }

        public Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            try
            {
                _lockSlim.EnterReadLock();
                if (_lastWrittenPosition == -1)
                    return Task.FromResult(0L);

                return Task.FromResult(_chunks[_lastWrittenPosition].Position);
            }
            finally
            {
                _lockSlim.ExitReadLock();
            }
        }

        public async Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId,
            CancellationToken cancellationToken)
        {
            if (index < 0)
            {
                throw new InvalidStreamIndexException(partitionId, index);
            }

            var id = Interlocked.Increment(ref _sequence);
            var chunk = new MemoryChunk()
            {
                Position = id,
                Index = index,
                OperationId = operationId ?? Guid.NewGuid().ToString(),
                PartitionId = partitionId,
                Payload = _cloneFunc(payload)
            };

            await _networkSimulator.Wait().ConfigureAwait(false);

            var partion = _partitions.GetOrAdd(partitionId,
                new InMemoryPartition(partitionId, _networkSimulator, Clone)
            );

            try
            {
                var chunkWritten = partion.Write(chunk);
                if (!chunkWritten)
                {
                    //idempotency on operationId.
                    return null;
                }
            }
            catch (DuplicateStreamIndexException)
            {
                // write empty chunk
                // keep same id to avoid holes in the stream
                chunk.PartitionId = EmptyPartitionId;
                chunk.Index = chunk.Position;
                chunk.OperationId = chunk.Position.ToString();
                chunk.Payload = null;
                _emptyInMemoryPartition.Write(chunk);
                SetChunk(chunk);
                throw;
            }

            SetChunk(chunk);
            await _networkSimulator.Wait().ConfigureAwait(false);

            return chunk;
        }

        public async Task<IChunk> ReplaceOneAsync(
            long position,
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            if (index < 0)
            {
                throw new InvalidStreamIndexException(partitionId, index);
            }

            var chunk = new MemoryChunk()
            {
                Position = position,
                Index = index,
                OperationId = operationId ?? Guid.NewGuid().ToString(),
                PartitionId = partitionId,
                Payload = _cloneFunc(payload)
            };

            await _networkSimulator.Wait().ConfigureAwait(false);

            try
            {
                _lockSlim.EnterWriteLock();
                var chunkIndexInGlobalArray = position - 1;
                if (_lastWrittenPosition < chunkIndexInGlobalArray)
                {
                    throw new InvalidOperationException("Chunk not found");
                }

                var previousChunk = _chunks[chunkIndexInGlobalArray];
                if (previousChunk.Position != position)
                {
                    throw new Exception("Internal error. InMemory Persistence Corruption");
                }

                // write new
                var partion = _partitions.GetOrAdd(partitionId,
                    new InMemoryPartition(partitionId, _networkSimulator, Clone)
                );

                try
                {
                    var chunkWritten = partion.Write(chunk);
                    if (!chunkWritten)
                    {
                        //idempotency on operationId.
                        return null;
                    }
                }
                catch (DuplicateStreamIndexException)
                {
                    // write empty chunk
                    // keep same id to avoid holes in the stream
                    chunk.PartitionId = EmptyPartitionId;
                    chunk.Index = chunk.Position;
                    chunk.OperationId = chunk.Position.ToString();
                    chunk.Payload = null;
                    _emptyInMemoryPartition.Write(chunk);

                    // update global
                    _chunks[chunkIndexInGlobalArray] = chunk;

                    // remove old
                    _partitions[previousChunk.PartitionId]
                        .Delete(previousChunk.Index, previousChunk.Index);

                    throw;
                }

                // update global
                _chunks[chunkIndexInGlobalArray] = chunk;

                // remove old
                _partitions[previousChunk.PartitionId]
                    .Delete(previousChunk.Index, previousChunk.Index);

                await _networkSimulator.Wait().ConfigureAwait(false);

                return chunk;
            }
            finally
            {
                _lockSlim.ExitWriteLock();
            }
        }

        public async Task<IChunk> ReadOneAsync(long position, CancellationToken cancellationToken)
        {
            try
            {
                await _networkSimulator.Wait().ConfigureAwait(false);
                _lockSlim.EnterReadLock();

                var globalIndex = position - 1;

                if (globalIndex > _lastWrittenPosition)
                {
                    return null;
                }

                var chunk = _chunks[globalIndex];
                if (chunk == null || chunk.Deleted)
                {
                    return null;
                }

                return Clone(chunk);
            }
            finally
            {
                _lockSlim.ExitReadLock();
            }
        }

        private void SetChunk(MemoryChunk chunk)
        {
            int slot = (int)chunk.Position - 1;

            _lockSlim.EnterWriteLock();
            try
            {
                _chunks[slot] = chunk;
                if (_lastWrittenPosition < slot)
                {
                    _lastWrittenPosition = slot;
                }
            }
            finally
            {
                _lockSlim.ExitWriteLock();
            }
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken
        )
        {
            await _networkSimulator.Wait().ConfigureAwait(false);

            if (!_partitions.TryGetValue(partitionId, out var partition))
            {
                return;
            }

            var deleted = partition.Delete(fromLowerIndexInclusive, toUpperIndexInclusive);

            foreach (var d in deleted)
            {
                d.Deleted = true;
            }
        }

        public async Task<IChunk> ReadByOperationIdAsync(
            string partitionId,
            string operationId,
            CancellationToken cancellationToken)
        {
            await _networkSimulator.Wait().ConfigureAwait(false);

            if (!_partitions.TryGetValue(partitionId, out var partition))
            {
                return null;
            }

            return await partition.GetByOperationId(operationId).ConfigureAwait(false);
        }

        public async Task ReadAllByOperationIdAsync(
            string operationId,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            await _networkSimulator.Wait().ConfigureAwait(false);
            var filter = new SubscriptionWrapper(subscription)
            {
                ChunkFilter = chunk => chunk.OperationId == operationId
            };

            await ReadAllAsync(0, filter, int.MaxValue, cancellationToken).ConfigureAwait(false);
        }

        public async Task AppendBatchAsync(WriteJob[] queue, CancellationToken cancellationToken)
        {
            if (queue == null)
                throw new ArgumentNullException(nameof(queue));

            foreach (var job in queue)
            {
                try
                {
                    var chunk = await AppendAsync(
                        job.PartitionId,
                        job.Index,
                        job.Payload,
                        job.OperationId,
                        cancellationToken
                    ).ConfigureAwait(false);

                    if (chunk != null)
                    {
                        job.Succeeded(chunk);
                    }
                    else
                    {
                        // Idempotent operation (same operation ID already exists)
                        job.Failed(WriteJob.WriteResult.DuplicatedOperation);
                    }
                }
                catch (DuplicateStreamIndexException)
                {
                    // Concurrency conflict
                    job.Failed(WriteJob.WriteResult.DuplicatedIndex);
                }
                catch (Exception)
                {
                    // Generic failure
                    job.Failed(WriteJob.WriteResult.Failed);
                }
            }
        }

        #region IPartitionPersistenceSync

        public IReadOnlyList<IChunk> ReadForward(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            int limit)
        {
            if (limit <= 0)
                return Array.Empty<IChunk>();

            if (partitionId == null)
                throw new ArgumentNullException(nameof(partitionId));

            if (!_partitions.TryGetValue(partitionId, out var partition))
                return Array.Empty<IChunk>();

            return partition.ReadForwardSync(fromLowerIndexInclusive, toUpperIndexInclusive, limit, Clone);
        }

        public IReadOnlyList<IChunk> ReadBackward(
            string partitionId,
            long fromUpperIndexInclusive,
            long toLowerIndexInclusive,
            int limit)
        {
            if (limit <= 0)
                return Array.Empty<IChunk>();

            if (partitionId == null)
                throw new ArgumentNullException(nameof(partitionId));

            if (!_partitions.TryGetValue(partitionId, out var partition))
                return Array.Empty<IChunk>();

            return partition.ReadBackwardSync(fromUpperIndexInclusive, toLowerIndexInclusive, limit, Clone);
        }

        public IChunk ReadSingleBackward(
            string partitionId,
            long fromUpperIndexInclusive)
        {
            if (partitionId == null)
                throw new ArgumentNullException(nameof(partitionId));

            if (!_partitions.TryGetValue(partitionId, out var partition))
                return null;

            return partition.PeekSync(fromUpperIndexInclusive, Clone);
        }

        public IChunk ReadByOperationId(
            string partitionId,
            string operationId)
        {
            if (partitionId == null)
                throw new ArgumentNullException(nameof(partitionId));

            if (!_partitions.TryGetValue(partitionId, out var partition))
                return null;

            return partition.GetByOperationIdSync(operationId, Clone);
        }

        #endregion

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                foreach (var partition in _partitions.Values)
                {
                    try
                    {
                        partition?.Dispose();
                    }
                    catch
                    {
                        // swallow to continue disposing other partitions
                    }
                }
                _partitions.Clear();
                _lockSlim?.Dispose();
            }

            _disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
