using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Tpl
{
    public class PersistenceBatcher : IPersistence
    {
        private readonly IPersistence _persistence;

        public PersistenceBatcher(IPersistence persistence)
        {
            _persistence = persistence;
        }

        public bool SupportsFillers => _persistence.SupportsFillers;

        public Task ReadForwardAsync(string partitionId, long fromLowerIndexInclusive, ISubscription subscription,
            long toUpperIndexInclusive, int limit, CancellationToken cancellationToken)
        {
            return _persistence.ReadForwardAsync(partitionId, fromLowerIndexInclusive, subscription, toUpperIndexInclusive, limit, cancellationToken);
        }

        public Task ReadBackwardAsync(string partitionId, long fromUpperIndexInclusive, ISubscription subscription,
            long toLowerIndexInclusive, int limit, CancellationToken cancellationToken)
        {
            return _persistence.ReadBackwardAsync(partitionId, fromUpperIndexInclusive, subscription, toLowerIndexInclusive, limit, cancellationToken);
        }

        public Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive, CancellationToken cancellationToken)
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

        public Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId,
            CancellationToken cancellationToken)
        {
            return _persistence.AppendAsync(partitionId, index, payload, operationId, cancellationToken);
        }

        public Task DeleteAsync(string partitionId, long fromLowerIndexInclusive, long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            return _persistence.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive, cancellationToken);
        }
    }
}
