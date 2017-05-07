using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public interface IStream
    {
        Task Append(string payload, string operationId = null);
        Task Read(
            int fromIndexInclusive,
            int toIndexInclusive,
            Func<long, object, ScanCallbackResult> consumer,
            CancellationToken cancellationToken = default(CancellationToken)
        );
        Task Delete();
    }
}