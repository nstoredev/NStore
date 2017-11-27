using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Core.Streams
{
    public interface IReadOnlyStream
    {
        string Id { get; }

        Task ReadAsync(
            ISubscription subscription,
            long fromIndexInclusive,
            long toIndexInclusive,
            CancellationToken cancellationToken
        );

        Task<bool> IsEmpty(CancellationToken cancellationToken);
        Task<bool> ContainsOperationAsync(string operationId, CancellationToken cancellationToken);
    }
}