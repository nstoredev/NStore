using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NStore.Persistence
{
    public class LogDecorator : IPersistence
    {
        private readonly IPersistence _persistence;
        private readonly ILogger _logger;

        public LogDecorator(IPersistence persistence, ILoggerFactory loggerFactory)
        {
            _persistence = persistence;
            _logger = loggerFactory.CreateLogger("Persistence");
        }

        public bool SupportsFillers => _persistence.SupportsFillers;

        public async Task ReadForwardAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start ReadPartitionForward(Partition {ParitionId}, from: {from})", partitionId, fromLowerIndexInclusive);
            await _persistence.ReadForwardAsync(partitionId, fromLowerIndexInclusive, subscription, toUpperIndexInclusive, limit, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("End ReadPartitionForward(Partition {ParitionId}, from: {from})", partitionId, fromLowerIndexInclusive);
        }

        public async Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start ReadPartitionBackward(Partition {ParitionId}, from: {from})", partitionId, fromUpperIndexInclusive);
            await _persistence.ReadBackwardAsync(partitionId, fromUpperIndexInclusive, subscription, toLowerIndexInclusive, limit, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("End ReadPartitionBackward(Partition {ParitionId}, from: {from})", partitionId, fromUpperIndexInclusive);
        }

        public async Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive, CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start ReadLast(partitionId:{partitionId}, to:{to})", partitionId, fromUpperIndexInclusive);
            var result = await _persistence.ReadSingleBackwardAsync(partitionId, fromUpperIndexInclusive, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("End ReadLast(partitionId:{partitionId}, to:{to})", partitionId, fromUpperIndexInclusive);
            return result;
        }

        public async Task ReadAllAsync(
            long fromSequenceIdInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start ReadAllAsync(from:{from}, limit:{limit})", fromSequenceIdInclusive, limit);
            await _persistence.ReadAllAsync(fromSequenceIdInclusive, subscription, limit, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("end ReadAllAsync(from:{from}, limit:{limit})", fromSequenceIdInclusive, limit);
        }

        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start ReadLastPosition()");
            var result = await _persistence.ReadLastPositionAsync(cancellationToken).ConfigureAwait(false);
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
            _logger.LogDebug("Start PersistAsync({partitionId}, {index})", partitionId, index);
            var result = await _persistence.AppendAsync(partitionId, index, payload, operationId, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("End PersistAsync({partitionId}, {index}) => {Position}", partitionId, index, result?.Position);
            return result;
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            _logger.LogDebug("Start DeleteAsync({partitionId}, {from}, {to})", partitionId, fromLowerIndexInclusive, toUpperIndexInclusive);
            await _persistence.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("End DeleteAsync({partitionId}, {from}, {to})", partitionId, fromLowerIndexInclusive, toUpperIndexInclusive);
        }
    }
}
