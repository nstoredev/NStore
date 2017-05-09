using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public interface IReadOnlyStream
    {
        Task Read(
            IConsumer consumer, 
            int fromIndexInclusive = 0, 
            int toIndexInclusive = Int32.MaxValue, 
            CancellationToken cancellationToken = default(CancellationToken)
        );
    }

    public interface IStream : IReadOnlyStream
    {
        Task Append(object payload, string operationId = null, CancellationToken cancellation = default(CancellationToken));
        Task Delete(CancellationToken cancellation = default(CancellationToken));
    }
}