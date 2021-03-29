using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using LiteDB;
using NStore.Core.Logging;
using NStore.Core.Persistence;

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
        private long _sequence = 0;

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
                        chunk.Payload = _options.PayloadSerializer.Deserialize((string) chunk.Payload);
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
                chunk.Payload = _options.PayloadSerializer.Deserialize((string) chunk.Payload);
            }

            return Task.FromResult((IChunk) chunk);
        }

        public async Task<IChunk> AppendAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            var chunk = new LiteDBChunk()
            {
                PartitionId = partitionId,
                Index = index,
                OperationId = operationId ?? Guid.NewGuid().ToString(),
                Payload = _options.PayloadSerializer.Serialize(payload)
            };

            for (int c = 0; c <= 10; c++)
            {
                if (index < 0)
                {
                    chunk.Index = Interlocked.Increment(ref _sequence);
                }

                chunk.StreamSequence = $"{chunk.PartitionId}-{chunk.Index}";
                chunk.StreamOperation = $"{chunk.PartitionId}-{chunk.OperationId}";

                try
                {
                    _streams.Insert(chunk);
                    return chunk;
                }
                catch (LiteException ex)
                {
                    if (ex.ErrorCode == LiteException.INDEX_DUPLICATE_KEY)
                    {
                        if (ex.Message.Contains(nameof(chunk.StreamOperation)))
                        {
                            return null;
                        }

                        if (ex.Message.Contains(nameof(chunk.StreamSequence)))
                        {
                            if (index < 0)
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                                await Task.Delay(c * 10, cancellationToken);

                                _sequence = await ReadLastPositionAsync(cancellationToken);
                                continue;
                            }

                            throw new DuplicateStreamIndexException(chunk.PartitionId, chunk.Index);
                        }
                    }

                    throw;
                }
            }

            throw new DuplicateStreamIndexException(chunk.PartitionId, chunk.Index);
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

        public Task<IChunk> ReadByOperationIdAsync(string partitionId, string operationId,
            CancellationToken cancellationToken)
        {
            var key = $"{partitionId}-{operationId}";

            IChunk chunk = _streams.Query().Where(x => x.StreamOperation == key).FirstOrDefault();
            return Task.FromResult(chunk);
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
        }

        public void DeleteDataFiles()
        {
            if (_db != null)
            {
                _streams = null;
                _db.Dispose();
            }

            // data file
            if (File.Exists(_options.ConnectionString))
            {
                File.Delete(_options.ConnectionString);
            }

            // log file
            var logFileName = Path.Combine
            (
                Path.GetDirectoryName(_options.ConnectionString),
                Path.ChangeExtension
                (
                    Path.GetFileNameWithoutExtension(_options.ConnectionString) + "-log",
                    Path.GetExtension(_options.ConnectionString)
                )
            );

            if (File.Exists(logFileName))
            {
                File.Delete(logFileName);
            }
        }

        public void Dispose()
        {
            _db.Dispose();
        }
    }
}