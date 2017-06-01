using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;

namespace NStore.Streams
{
    public interface IReadOnlyStream
    {
        Task Read(
            ISubscription subscription,
            int fromIndexInclusive,
            int toIndexInclusive,
            CancellationToken cancellationToken
        );
    }
}