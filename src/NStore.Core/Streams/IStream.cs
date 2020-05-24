using NStore.Core.Persistence;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Streams
{
    public interface IStream : IReadOnlyStream
    {
        bool IsWritable { get; }

        Task<IChunk> AppendAsync(
            object payload,
            string operationId,
            CancellationToken cancellation
        );

        Task DeleteAsync(
            CancellationToken cancellation
        );

        Task DeleteBeforeAsync(
            long index,
            CancellationToken cancellation
        );
    }
}