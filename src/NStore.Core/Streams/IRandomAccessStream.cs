using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Core.Streams
{
    public interface IRandomAccessStream : IStream
    {
        Task<IChunk> PersistAsync(
            object payload,
            long index,
            string operationId,
            CancellationToken cancellation
        );
    }
}