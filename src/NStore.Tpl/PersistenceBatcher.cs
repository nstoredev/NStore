using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using NStore.Core.Persistence;

namespace NStore.Tpl
{
    public class PersistenceBatcher : IPersistence, IDisposable
    {
        private readonly IPersistence _persistence;
        private readonly BatchBlock<AsyncWriteJob> _batch;
        private readonly CancellationTokenSource _cts;

        public PersistenceBatcher(IPersistence persistence, int batchSize, int flushTimeout = 10)
        {
            _cts = new CancellationTokenSource();
            var batcher = (IEnhancedPersistence) persistence;
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
                queue =>
                {
                    return batcher.AppendBatchAsync(queue, CancellationToken.None);
                }, 
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

        public void Cancel(int milliseconds)
        {
            _cts.CancelAfter(millisecondsDelay: milliseconds);
        }

        public Task ReadForwardAsync(string partitionId, long fromLowerIndexInclusive, ISubscription subscription,
            long toUpperIndexInclusive, int limit, CancellationToken cancellationToken)
        {
            return _persistence.ReadForwardAsync(partitionId, fromLowerIndexInclusive, subscription,
                toUpperIndexInclusive, limit, cancellationToken);
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
            return await job.Task;
            //            return _persistence.AppendAsync(partitionId, index, payload, operationId, cancellationToken);
        }

        public Task DeleteAsync(string partitionId, long fromLowerIndexInclusive, long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            return _persistence.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive,
                cancellationToken);
        }

        public void Dispose()
        {
            _batch.Complete();
            _batch.Completion.GetAwaiter().GetResult();
        }
    }
}