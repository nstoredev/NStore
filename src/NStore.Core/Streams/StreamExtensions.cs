using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Core.Streams
{
    public static class StreamExtensions
    {
        public static Task<IChunk> PersistAsync(this IRandomAccessStream stream, object payload)
        {
            return stream.PersistAsync(payload, -1, null, CancellationToken.None);
        }

        public static Task<IChunk> PersistAsync(this IRandomAccessStream stream, object payload, string operationId)
        {
            return stream.PersistAsync(payload, -1, operationId, CancellationToken.None);
        }

        public static Task<IChunk> PersistAsync(
            this IRandomAccessStream stream,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            return stream.PersistAsync(payload, -1, operationId, cancellationToken);
        }

        public static Task<IChunk> PersistAsync(this IRandomAccessStream stream, long index, object payload, string operationId)
        {
            return stream.PersistAsync(payload, index, operationId, CancellationToken.None);
        }

        public static Task<IChunk> AppendAsync(this IStream stream, object payload, string operationId)
        {
            return stream.AppendAsync(payload, operationId, CancellationToken.None);
        }

        public static Task<IChunk> AppendAsync(this IStream stream, object payload, string operationId, CancellationToken cancellationToken)
        {
            return stream.AppendAsync(payload, operationId, cancellationToken);
        }

        public static Task<IChunk> AppendAsync(this IStream stream, object payload)
        {
            return stream.AppendAsync(payload, null, CancellationToken.None);
        }

        public static Task DeleteAsync(this IStream stream)
        {
            return stream.DeleteAsync(CancellationToken.None);
        }

        public static Task<bool> ContainsOperationAsync(this IStream stream, string operationId)
        {
            return stream.ContainsOperationAsync(operationId, CancellationToken.None);
        }

        public static Task<IChunk> PeekAsync(this IStream stream)
        {
            return stream.PeekAsync(CancellationToken.None);
        }
    }
}