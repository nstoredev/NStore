using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.LoadTests
{
    public class MetricsPersistenceDecorator : IPersistence
    {
        private readonly IPersistence _persistence;

        public MetricsPersistenceDecorator(IPersistence persistence)
        {
            _persistence = persistence;
        }

        public bool SupportsFillers => _persistence.SupportsFillers;

        public Task ReadForwardAsync(string partitionId, long fromLowerIndexInclusive, ISubscription subscription,
            long toUpperIndexInclusive, int limit, CancellationToken cancellationToken)
        {
            return Track.Profile(
                Timers.ReadForward,
                () => _persistence.ReadForwardAsync(partitionId, fromLowerIndexInclusive, subscription,
                    toUpperIndexInclusive, limit, cancellationToken)
            );
        }

        public Task ReadBackwardAsync(string partitionId, long fromUpperIndexInclusive, ISubscription subscription,
            long toLowerIndexInclusive, int limit, CancellationToken cancellationToken)
        {
            return Track.Profile(Timers.ReadBackward, () =>
                _persistence.ReadBackwardAsync(partitionId, fromUpperIndexInclusive, subscription,
                    toLowerIndexInclusive, limit, cancellationToken)
            );
        }

        public Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            return Track.Profile(Timers.ReadSingleBackward, () =>
                _persistence.ReadSingleBackwardAsync(partitionId, fromUpperIndexInclusive, cancellationToken)
            );
        }

        public Task ReadAllAsync(long fromPositionInclusive, ISubscription subscription, int limit,
            CancellationToken cancellationToken)
        {
            return Track.Profile(Timers.ReadAll, () =>
                _persistence.ReadAllAsync(fromPositionInclusive, subscription, limit, cancellationToken)
            );
        }

        public Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            return Track.Profile(Timers.ReadLastPosition, () =>
                _persistence.ReadLastPositionAsync(cancellationToken)
            );
        }

        public Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId,
            CancellationToken cancellationToken)
        {
            return Track.Profile(Timers.Append, () =>
                _persistence.AppendAsync(partitionId, index, payload, operationId, cancellationToken)
            );
        }

        public Task DeleteAsync(string partitionId, long fromLowerIndexInclusive, long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            return Track.Profile(Timers.Delete, () =>
                _persistence.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive,
                    cancellationToken)
            );
        }

        public Task<IChunk> ReadByOperationIdAsync(string partitionId, string operationId,
            CancellationToken cancellationToken)
        {
            return Track.Profile(Timers.ReadByOperationId, () =>
                _persistence.ReadByOperationIdAsync(partitionId, operationId, cancellationToken)
            );
        }

        public Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription,
            CancellationToken cancellationToken)
        {
            return Track.Profile(Timers.ReadAllByOperationId, () =>
                _persistence.ReadAllByOperationIdAsync(operationId, subscription, cancellationToken)
            );
        }
    }
}