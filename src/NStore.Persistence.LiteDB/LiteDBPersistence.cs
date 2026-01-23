using LiteDB;
using NStore.Core.Logging;
using NStore.Core.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence.LiteDB
{
    /// <summary>
    /// LiteDB Persistence Provider
    /// </summary>
    public class LiteDBPersistence : IPersistence, IDisposable
    {
        private readonly LiteDBPersistenceOptions _options;
        private LiteDatabase _db;
        private ILiteCollection<LiteDBChunk> _streams;
        private readonly INStoreLogger _logger;

        public LiteDBPersistence(LiteDBPersistenceOptions options)
        {
            _options = options;
            _logger = _options.LoggerFactory.CreateLogger(_options.ConnectionString);

            _logger.LogInformation("LiteDB Persistence on {file}", _options.ConnectionString);
        }

        public bool SupportsFillers => false;

        public async Task ReadForwardAsync
        (
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            var chunks = _streams.Query()
                .Where(x => x.PartitionId == partitionId
                            && x.Index >= fromLowerIndexInclusive
                            && x.Index <= toUpperIndexInclusive)
                .OrderBy(x => x.Index)
                .Limit(limit)
                .ToList();

            await PublishAsync(chunks, fromLowerIndexInclusive, subscription, false, cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task ReadForwardMultiplePartitionsAsync(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            var chunks = _streams.Query()
               .Where(x => partitionIdsList.Contains(x.PartitionId)
                           && x.Index >= fromLowerIndexInclusive
                           && x.Index <= toUpperIndexInclusive)
               .OrderBy(x => x.Index)
               .ToList();

            await PublishAsync(chunks, fromLowerIndexInclusive, subscription, false, cancellationToken)
                .ConfigureAwait(false);
        }

#if NET8_0_OR_GREATER
        public async IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsAsyncEnumerable(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var partitionsList = partitionIdsList.ToList();
            var query = _streams.Query()
               .Where(x => partitionsList.Contains(x.PartitionId)
                           && x.Index >= fromLowerIndexInclusive
                           && x.Index <= toUpperIndexInclusive)
               .OrderBy(x => x.Index);

            await Task.CompletedTask;

            foreach (var chunk in query.ToEnumerable())
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return chunk;
            }
        }
#endif

        /// <summary>
        /// Reads multiple partitions where each partition can have its own index range.
        /// Implements subscription-based result delivery with per-partition ordering guarantees.
        /// </summary>
        /// <param name="partitionRequests">List of partition read requests, each with its own range.</param>
        /// <param name="subscription">Subscriber that will receive chunks.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <remarks>
        /// Chunks within the same partition are ordered by partition index.
        /// Overlapping or duplicate ranges for the same partition are automatically merged and deduplicated.
        /// NO temporal ordering is guaranteed between different partitions.
        /// </remarks>
        public async Task ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            var chunks = GatherChunksFromPartitionRequests(partitionRequests, deserializePayloads: false);
            var startIndex = partitionRequests.Any() 
                ? partitionRequests.Min(r => r.FromPartitionIndexInclusive) 
                : 0L;

            await PublishAsync(chunks, startIndex, subscription, false, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously enumerates chunks for multiple partitions where each partition can have its own index range.
        /// Consumers can use `await foreach` and stop enumeration early to signal the producer to stop.
        /// </summary>
        /// <param name="partitionRequests">List of partition read requests, each with its own range.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>An async sequence of <see cref="IChunk"/> instances.</returns>
        /// <remarks>
        /// Chunks within the same partition are ordered by partition index.
        /// Overlapping or duplicate ranges for the same partition are automatically merged and deduplicated.
        /// NO temporal ordering is guaranteed between different partitions.
        /// </remarks>
        public async IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var requestsList = partitionRequests.ToList();
            if (!requestsList.Any())
            {
                yield break;
            }

            // Group requests by partition to preserve per-partition ordering and handle duplicates
            var partitionGroups = requestsList
                .GroupBy(r => r.PartitionId)
                .Select(g => new
                {
                    PartitionId = g.Key,
                    Ranges = OptimizeRanges(g.Select(r => (r.FromPartitionIndexInclusive, r.ToPartitionIndexInclusive)).ToList())
                })
                .ToList();

            var seenChunks = new HashSet<(string PartitionId, long Index)>();

            await Task.CompletedTask;

            foreach (var partitionGroup in partitionGroups)
            {
                // Query each optimized range for this partition
                foreach (var (fromIndex, toIndex) in partitionGroup.Ranges)
                {
                    var rangeChunks = _streams.Query()
                        .Where(x => x.PartitionId == partitionGroup.PartitionId
                                    && x.Index >= fromIndex
                                    && x.Index <= toIndex)
                        .OrderBy(x => x.Index)
                        .ToEnumerable();

                    foreach (var chunk in rangeChunks)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var key = (chunk.PartitionId, chunk.Index);
                        if (seenChunks.Add(key))
                        {
                            yield return chunk;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Helper method to gather chunks from partition requests with range optimization and deduplication.
        /// </summary>
        /// <param name="partitionRequests">List of partition read requests.</param>
        /// <param name="deserializePayloads">Whether to deserialize payloads (true for direct consumption, false when PublishAsync will handle it).</param>
        /// <returns>List of deduplicated chunks ordered by partition and index.</returns>
        private List<LiteDBChunk> GatherChunksFromPartitionRequests(
            IEnumerable<PartitionReadRequest> partitionRequests, 
            bool deserializePayloads)
        {
            var requestsList = partitionRequests.ToList();
            if (!requestsList.Any())
            {
                return new List<LiteDBChunk>();
            }

            // Group requests by partition to preserve per-partition ordering and handle duplicates
            var partitionGroups = requestsList
                .GroupBy(r => r.PartitionId)
                .Select(g => new
                {
                    PartitionId = g.Key,
                    Ranges = OptimizeRanges(g.Select(r => (r.FromPartitionIndexInclusive, r.ToPartitionIndexInclusive)).ToList())
                })
                .ToList();

            var allChunks = new List<LiteDBChunk>();
            var seenChunks = new HashSet<(string PartitionId, long Index)>();

            foreach (var partitionGroup in partitionGroups)
            {
                var partitionChunks = new List<LiteDBChunk>();

                // Query each optimized range for this partition
                foreach (var (fromIndex, toIndex) in partitionGroup.Ranges)
                {
                    var rangeChunks = _streams.Query()
                        .Where(x => x.PartitionId == partitionGroup.PartitionId
                                    && x.Index >= fromIndex
                                    && x.Index <= toIndex)
                        .OrderBy(x => x.Index)
                        .ToList();

                    partitionChunks.AddRange(rangeChunks);
                }

                // Deduplicate and maintain ordering within partition
                foreach (var chunk in partitionChunks.OrderBy(c => c.Index))
                {
                    var key = (chunk.PartitionId, chunk.Index);
                    if (seenChunks.Add(key))
                    {
                        // Optionally deserialize payload if requested
                        if (deserializePayloads && chunk.Payload != null)
                        {
                            chunk.Payload = _options.PayloadSerializer.Deserialize((string)chunk.Payload);
                        }
                        allChunks.Add(chunk);
                    }
                }
            }

            return allChunks;
        }

        /// <summary>
        /// Optimizes a list of index ranges by merging overlapping or contiguous ranges.
        /// This reduces the number of database queries needed.
        /// </summary>
        /// <param name="ranges">List of ranges to optimize.</param>
        /// <returns>Optimized list with merged ranges.</returns>
        private List<(long FromIndex, long ToIndex)> OptimizeRanges(List<(long FromIndex, long ToIndex)> ranges)
        {
            if (ranges.Count <= 1)
            {
                return ranges;
            }

            // Sort ranges by start index
            var sortedRanges = ranges.OrderBy(r => r.FromIndex).ToList();
            var optimized = new List<(long FromIndex, long ToIndex)>();

            var currentRange = sortedRanges[0];

            for (int i = 1; i < sortedRanges.Count; i++)
            {
                var nextRange = sortedRanges[i];

                // Check if ranges overlap or are contiguous
                if (nextRange.FromIndex <= currentRange.ToIndex + 1)
                {
                    // Merge ranges
                    currentRange = (currentRange.FromIndex, Math.Max(currentRange.ToIndex, nextRange.ToIndex));
                }
                else
                {
                    // No overlap, add current range and move to next
                    optimized.Add(currentRange);
                    currentRange = nextRange;
                }
            }

            // Add the last range
            optimized.Add(currentRange);

            return optimized;
        }

        private async Task PublishAsync(
            IEnumerable<LiteDBChunk> chunks,
            long start,
            ISubscription subscription,
            bool broadcastPosition,
            CancellationToken cancellationToken)
        {
            await subscription.OnStartAsync(start).ConfigureAwait(false);

            long positionOrIndex = 0;

            try
            {
                foreach (var chunk in chunks)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    positionOrIndex = broadcastPosition ? chunk.Position : chunk.Index;

                    if (chunk.Payload != null)
                    {
                        chunk.Payload = _options.PayloadSerializer.Deserialize((string)chunk.Payload);
                    }

                    var ok = await subscription.OnNextAsync(chunk).ConfigureAwait(false);
                    if (!ok)
                    {
                        await subscription.StoppedAsync(positionOrIndex).ConfigureAwait(false);
                        return;
                    }
                }
            }
            catch (TaskCanceledException ex)
            {
                _logger.LogWarning($"PushToSubscriber: {ex.Message}.\n{ex.StackTrace}");
                await subscription.StoppedAsync(positionOrIndex).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.LogError($"Error During PushToSubscriber: {e.Message}.\n{e.StackTrace}");
                await subscription.OnErrorAsync(positionOrIndex, e).ConfigureAwait(false);
            }

            await subscription.CompletedAsync(positionOrIndex).ConfigureAwait(false);
        }

        public async Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            var chunks = _streams.Query()
                .Where(x => x.PartitionId == partitionId && x.Index >= toLowerIndexInclusive &&
                            x.Index <= fromUpperIndexInclusive)
                .OrderByDescending(x => x.Index)
                .Limit(limit)
                .ToEnumerable();

            await PublishAsync(chunks, fromUpperIndexInclusive, subscription, false, cancellationToken)
                .ConfigureAwait(false);
        }

        public Task<IChunk> ReadSingleBackwardAsync
        (
            string partitionId,
            long fromUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            var chunk = _streams.Query()
                .Where(x => x.PartitionId == partitionId && x.Index <= fromUpperIndexInclusive)
                .OrderByDescending(x => x.Index)
                .Limit(1)
                .FirstOrDefault();

            if (chunk?.Payload != null)
            {
                chunk.Payload = _options.PayloadSerializer.Deserialize((string)chunk.Payload);
            }

            return Task.FromResult((IChunk)chunk);
        }

        public Task<IChunk> AppendAsync(
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

            var chunk = new LiteDBChunk()
            {
                PartitionId = partitionId,
                Index = index,
                OperationId = operationId ?? Guid.NewGuid().ToString(),
                Payload = _options.PayloadSerializer.Serialize(payload)
            };

            chunk.StreamSequence = $"{chunk.PartitionId}-{chunk.Index}";
            chunk.StreamOperation = $"{chunk.PartitionId}-{chunk.OperationId}";

            try
            {
                _streams.Insert(chunk);
                return Task.FromResult((IChunk)chunk);
            }
            catch (LiteException ex)
            {
                if (ex.ErrorCode != LiteException.INDEX_DUPLICATE_KEY)
                {
                    throw;
                }

                if (ex.Message.Contains(nameof(chunk.StreamOperation)))
                {
                    return Task.FromResult((IChunk)null);
                }

                if (ex.Message.Contains(nameof(chunk.StreamSequence)))
                {
                    throw new DuplicateStreamIndexException(chunk.PartitionId, chunk.Index);
                }

                throw;
            }
        }

        public Task<IChunk> ReplaceOneAsync
        (
            long position,
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            var chunk = new LiteDBChunk()
            {
                Position = position,
                PartitionId = partitionId,
                Index = index,
                OperationId = operationId ?? Guid.NewGuid().ToString(),
                Payload = _options.PayloadSerializer.Serialize(payload)
            };

            chunk.StreamSequence = $"{chunk.PartitionId}-{chunk.Index}";
            chunk.StreamOperation = $"{chunk.PartitionId}-{chunk.OperationId}";

            try
            {
                var updated = _streams.Update(chunk);
                if (!updated)
                {
                    throw new Exception("Cannot rewrite chunk");
                }

                return Task.FromResult<IChunk>(chunk);
            }
            catch (LiteException ex)
            {
                if (ex.ErrorCode != LiteException.INDEX_DUPLICATE_KEY)
                {
                    throw;
                }

                if (ex.Message.Contains(nameof(chunk.StreamOperation)))
                {
                    return Task.FromResult((IChunk)null);
                }

                if (ex.Message.Contains(nameof(chunk.StreamSequence)))
                {
                    throw new DuplicateStreamIndexException(chunk.PartitionId, chunk.Index);
                }

                throw;
            }
        }

        public Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            _streams.DeleteMany(x =>
                x.PartitionId == partitionId
                && x.Index >= fromLowerIndexInclusive
                && x.Index <= toUpperIndexInclusive
            );

            return Task.CompletedTask;
        }

        public Task<IChunk> ReadByOperationIdAsync(
            string partitionId,
            string operationId,
            CancellationToken cancellationToken)
        {
            var key = $"{partitionId}-{operationId}";

            var chunk = _streams.Query().Where(x => x.StreamOperation == key).FirstOrDefault();
            if (chunk?.Payload != null)
            {
                chunk.Payload = _options.PayloadSerializer.Deserialize((string)chunk.Payload);
            }

            return Task.FromResult<IChunk>(chunk);
        }

        public Task<IChunk> ReadOneAsync(long position, CancellationToken cancellationToken)
        {
            var chunk = _streams.Query().Where(x => x.Position == position).FirstOrDefault();
            if (chunk?.Payload != null)
            {
                chunk.Payload = _options.PayloadSerializer.Deserialize((string)chunk.Payload);
            }
            return Task.FromResult<IChunk>(chunk);
        }

        public async Task ReadAllAsync
        (
            long fromPositionInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken)
        {
            var chunks = _streams.Query()
                .Where(x => x.Position >= fromPositionInclusive)
                .Limit(limit)
                .ToEnumerable();

            await PublishAsync(chunks, fromPositionInclusive, subscription, true, cancellationToken)
                .ConfigureAwait(false);
        }

        public Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            var lastPosition = _streams.Query()
                .OrderByDescending(x => x.Position)
                .Select(x => x.Position)
                .FirstOrDefault();

            return Task.FromResult(lastPosition);
        }

        public async Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription,
            CancellationToken cancellationToken)
        {
            var chunks = _streams.Query()
                .Where(x => x.OperationId == operationId)
                .OrderBy(x => x.Position)
                .ToEnumerable();

            await PublishAsync(chunks, 0, subscription, true, cancellationToken).ConfigureAwait(false);
        }

        public void Init()
        {
            _db = new LiteDatabase(_options.ConnectionString, _options.Mapper);
            _streams = _db.GetCollection<LiteDBChunk>(_options.StreamsCollectionName);

            _streams.EnsureIndex(x => x.StreamSequence, true);
            _streams.EnsureIndex(x => x.StreamOperation, true);
            _streams.EnsureIndex(x => x.PartitionId, false);
            _streams.EnsureIndex(x => x.Index, false);
        }

        public void Dispose()
        {
            _db.Dispose();
        }
    }
}