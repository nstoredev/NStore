using System.Data;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Logging;

namespace NStore.Core.Persistence
{
    public class LogDecorator : IPersistence
    {
        private readonly IPersistence _store;
        private readonly INStoreLogger _logger;

        public LogDecorator(IPersistence store, INStoreLoggerFactory inStoreLoggerFactory, string categoryName = "Persistence")
        {
            _store = store;
            _logger = inStoreLoggerFactory.CreateLogger(categoryName);
        }

        public bool SupportsFillers => _store.SupportsFillers;

        public async Task ReadForwardAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start ReadPartitionForward(Partition {PartitionId}, from: {from})", partitionId, fromLowerIndexInclusive);
            await _store.ReadForwardAsync(partitionId, fromLowerIndexInclusive, subscription, toUpperIndexInclusive, limit, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("End ReadPartitionForward(Partition {PartitionId}, from: {from})", partitionId, fromLowerIndexInclusive);
        }

        public async Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start ReadPartitionBackward(Partition {PartitionId}, from: {from})", partitionId, fromUpperIndexInclusive);
            await _store.ReadBackwardAsync(partitionId, fromUpperIndexInclusive, subscription, toLowerIndexInclusive, limit, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("End ReadPartitionBackward(Partition {PartitionId}, from: {from})", partitionId, fromUpperIndexInclusive);
        }

        public async Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive, CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start ReadLast(partitionId:{partitionId}, to:{to})", partitionId, fromUpperIndexInclusive);
            var result = await _store.ReadSingleBackwardAsync(partitionId, fromUpperIndexInclusive, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("End ReadLast(partitionId:{partitionId}, to:{to})", partitionId, fromUpperIndexInclusive);
            return result;
        }

        public async Task ReadAllAsync(
            long fromPositionInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start ReadAllAsync(from:{from}, limit:{limit})", fromPositionInclusive, limit);
            await _store.ReadAllAsync(fromPositionInclusive, subscription, limit, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("end ReadAllAsync(from:{from}, limit:{limit})", fromPositionInclusive, limit);
        }

        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start ReadLastPosition()");
            var result = await _store.ReadLastPositionAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("end ReadLastPosition()");
            return result;
        }

        public async Task<IChunk> AppendAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start PersistAsync(partition: \"{partitionId}\", index: {index}, op: \"{op}\")", partitionId, index, operationId);
            var result = await _store.AppendAsync(partitionId, index, payload, operationId, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("End PersistAsync(partition: \"{partitionId}\", index: {index}, op: \"{op}\") => position: {Position}", partitionId, index, operationId, result?.Position);
            return result;
        }

        public async Task<IChunk> ReplaceOneAsync(long position, string partitionId, long index, object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start RewriteAsync({position}, {partitionId}, {index})", position, partitionId, index);
           var chunk =  await _store.ReplaceOneAsync(position, partitionId, index, payload, operationId, cancellationToken);
           _logger.LogDebug("End RewriteAsync({position}, {partitionId}, {index})", position, partitionId, index);
           return chunk;
        }

        public async Task<IChunk> ReadOneAsync(long position, CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start ReadOneAsync({position})", position);
            var chunk =  await _store.ReadOneAsync(position, cancellationToken);
            _logger.LogDebug("End ReadOneAsync({position})", position);
            return chunk;
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start DeleteAsync({partitionId}, {from}, {to})", partitionId, fromLowerIndexInclusive, toUpperIndexInclusive);
            await _store.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("End DeleteAsync({partitionId}, {from}, {to})", partitionId, fromLowerIndexInclusive, toUpperIndexInclusive);
        }

        public Task<IChunk> ReadByOperationIdAsync(string partitionId, string operationId, CancellationToken cancellationToken)
        {
            return _store.ReadByOperationIdAsync(partitionId, operationId, cancellationToken);
        }

        public Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription, CancellationToken cancellationToken)
        {
            return _store.ReadAllByOperationIdAsync(operationId, subscription, cancellationToken);
        }
    }
}
