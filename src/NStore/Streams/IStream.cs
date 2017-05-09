using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public interface IReadOnlyStream
    {
        Task Read(
            int fromIndexInclusive,
            int toIndexInclusive,
            Func<long, object, ScanCallbackResult> consumer,
            CancellationToken cancellationToken = default(CancellationToken)
        );
    }

    public interface IStream : IReadOnlyStream
    {
        Task Append(object payload, string operationId = null);
        Task Delete();
    }

    public interface IOptimisticConcurrencyStream : IReadOnlyStream
    {
        Task Append(long version, object payload, string operationId = null);
        Task Delete();
    }
}