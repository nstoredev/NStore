using NStore.Core.Persistence;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace NStore.Tpl
{
    public class PersistenceBatchAppendDecorator : IPersistence, IDisposable
    {
        private readonly IPersistence _persistence;
        private readonly BatchBlock<AsyncWriteJob> _batch;
        private readonly CancellationTokenSource _cts;

        public PersistenceBatchAppendDecorator(IPersistence persistence, int batchSize, int flushTimeout)
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

            var processor = new ActionBlock<AsyncWriteJob[]>
            (
                queue => batcher.AppendBatchAsync(queue, CancellationToken.None),
                new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = Environment.ProcessorCount,
                    BoundedCapacity = 1024,
                    CancellationToken = _cts.Token
                }
            );

            _batch.LinkTo(processor, new DataflowLinkOptions()
            {
                PropagateCompletion = true,
            });

            _persistence = persistence;
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

        public void Dispose()
        {
            _batch.Complete();
            _batch.Completion.GetAwaiter().GetResult();
            _cts.Cancel();
        }
    }
}