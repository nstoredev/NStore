using NStore.Core.Logging;
using NStore.Core.Persistence;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace NStore.Tpl
{
    /// <summary>
    /// Decorator that batches write operations for improved performance.
    /// Implements two-phase disposal: call <see cref="ShutdownAsync"/> for graceful async shutdown,
    /// then <see cref="Dispose"/> for synchronous cleanup.
    /// </summary>
    public class PersistenceBatchAppendDecorator : IPersistence, IDisposable, IAsyncDisposable
    {
        private readonly IPersistence _persistence;
        private readonly INStoreLogger _nStoreLogger;
        private readonly BatchBlock<AsyncWriteJob> _batch;
        private readonly ActionBlock<AsyncWriteJob[]> _processor;
        private readonly CancellationTokenSource _cts;
        private bool _disposed;

        public PersistenceBatchAppendDecorator(
            IPersistence persistence,
            INStoreLogger nStoreLogger,
            int batchSize,
            int flushTimeout)
        {
            _cts = new CancellationTokenSource();
            var batcher = (IEnhancedPersistence)persistence;
            _batch = new BatchBlock<AsyncWriteJob>(batchSize, new GroupingDataflowBlockOptions()
            {
                //                BoundedCapacity = 1024,
                CancellationToken = _cts.Token
            });

            Task.Run(async () =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    await Task.Delay(flushTimeout).ConfigureAwait(false);
                    _batch.TriggerBatch();
                }
            });

            _processor = new ActionBlock<AsyncWriteJob[]>
            (
                queue => batcher.AppendBatchAsync(queue, CancellationToken.None),
                new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = Environment.ProcessorCount,
                    BoundedCapacity = 1024,
                    CancellationToken = _cts.Token
                }
            );

            _batch.LinkTo(_processor, new DataflowLinkOptions()
            {
                PropagateCompletion = true,
            });

            _persistence = persistence;
            _nStoreLogger = nStoreLogger;
        }

        public bool SupportsFillers => _persistence.SupportsFillers;

        public Task ReadForwardAsync(string partitionId, long fromLowerIndexInclusive, ISubscription subscription,
            long toUpperIndexInclusive, int limit, CancellationToken cancellationToken)
        {
            return _persistence.ReadForwardAsync(partitionId, fromLowerIndexInclusive, subscription,
                toUpperIndexInclusive, limit, cancellationToken);
        }

        public Task ReadForwardMultiplePartitionsAsync(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            return _persistence.ReadForwardMultiplePartitionsAsync(
                partitionIdsList,
                fromLowerIndexInclusive,
                subscription,
                toUpperIndexInclusive,
                cancellationToken);
        }

        public Task ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            return _persistence.ReadForwardMultiplePartitionsWithRangesAsync(partitionRequests, subscription, cancellationToken);
        }

#if NET8_0_OR_GREATER
        public IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsAsyncEnumerable(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            return _persistence.ReadForwardMultiplePartitionsAsyncEnumerable(
                partitionIdsList,
                fromLowerIndexInclusive,
                toUpperIndexInclusive,
                cancellationToken);
        }
#endif

        public IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            CancellationToken cancellationToken = default)
        {
            return _persistence.ReadForwardMultiplePartitionsWithRangesAsync(partitionRequests, cancellationToken);
        }

        public Task ReadBackwardAsync(string partitionId, long fromUpperIndexInclusive, ISubscription subscription,
            long toLowerIndexInclusive, int limit, CancellationToken cancellationToken)
        {
            return _persistence.ReadBackwardAsync(partitionId, fromUpperIndexInclusive, subscription,
                toLowerIndexInclusive, limit, cancellationToken);
        }

        public Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            return _persistence.ReadSingleBackwardAsync(partitionId, fromUpperIndexInclusive, cancellationToken);
        }

        public Task ReadAllAsync(long fromPositionInclusive, ISubscription subscription, int limit,
            CancellationToken cancellationToken)
        {
            return _persistence.ReadAllAsync(fromPositionInclusive, subscription, limit, cancellationToken);
        }

        public Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            return _persistence.ReadLastPositionAsync(cancellationToken);
        }

        public async Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId,
            CancellationToken cancellationToken)
        {
            var job = new AsyncWriteJob(partitionId, index, payload, operationId);

            await _batch.SendAsync(job, cancellationToken).ConfigureAwait(false);
            return await job.Task.ConfigureAwait(false);
        }

        public Task<IChunk> ReplaceOneAsync(long position, string partitionId, long index, object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            return _persistence.ReplaceOneAsync(position, partitionId, index, payload, operationId, cancellationToken);
        }

        public Task<IChunk> ReadOneAsync(long position, CancellationToken cancellationToken)
        {
            return _persistence.ReadOneAsync(position, cancellationToken);
        }

        public Task DeleteAsync(string partitionId, long fromLowerIndexInclusive, long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            return _persistence.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive,
                cancellationToken);
        }

        public Task<IChunk> ReadByOperationIdAsync(string partitionId, string operationId, CancellationToken cancellationToken)
        {
            return _persistence.ReadByOperationIdAsync(partitionId, operationId, cancellationToken);
        }

        public Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription, CancellationToken cancellationToken)
        {
            return _persistence.ReadAllByOperationIdAsync(operationId, subscription, cancellationToken);
        }

        /// <summary>
        /// Gracefully shuts down the batching pipeline by completing the batch queue
        /// and waiting for all pending operations to finish.
        /// Call this method before <see cref="Dispose"/> to ensure all writes are flushed.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to abort the shutdown wait.</param>
        /// <returns>A task representing the async shutdown operation.</returns>
        public async Task ShutdownAsync(CancellationToken cancellationToken = default)
        {
            _batch.Complete();

            try
            {
                await _processor.Completion.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation token is triggered
            }
            _cts.Cancel();
            _cts.Dispose();
        }

        /// <summary>
        /// Disposes managed resources. For graceful shutdown, call <see cref="ShutdownAsync"/> first.
        /// This method performs non-blocking cleanup only.
        /// </summary>
        public void Dispose()
        {
            try
            {
                var task = ShutdownAsync();
                Task.WhenAny(task, Task.Delay(5000)).GetAwaiter().GetResult();
                if (!task.IsCompleted)
                {
                    _nStoreLogger?.LogWarning("PersistenceBatchAppendDecorator disposal timed out before completion.");
                }
            }
            catch (Exception ex)
            {
                _nStoreLogger?.LogWarning($"Exception while disposing PersistenceBatchAppendDecorator: {ex.Message}");
            }

            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            await ShutdownAsync().ConfigureAwait(false);
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                try
                {
                    _cts?.Cancel();
                }
                catch
                {
                }

                try
                {
                    _cts?.Dispose();
                }
                catch
                {
                }
            }

            _disposed = true;
        }
    }
}