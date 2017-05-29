using System.Threading;
using System.Threading.Tasks;

namespace NStore.Streams
{
    public static class StreamExtensions
    {
        public static Task Append(this IStream stream, object payload)
        {
            return stream.Append(payload, null, CancellationToken.None);
        }

        public static Task Append(this IStream stream, object payload, string operationId)
        {
            return stream.Append(payload, operationId, CancellationToken.None);
        }

        public static Task Delete(this IStream stream)
        {
            return stream.Delete(CancellationToken.None);
        }
    }
}