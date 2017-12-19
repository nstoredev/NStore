using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public interface IGlobalPersistence
    {
        Task ReadAllAsync(
            long fromPositionInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken
        );

        Task<long> ReadLastPositionAsync(
            CancellationToken cancellationToken
        );

        Task ReadAllByOperationIdAsync(
            string operationId,
            ISubscription subscription,
            CancellationToken cancellationToken
        );
    }
}