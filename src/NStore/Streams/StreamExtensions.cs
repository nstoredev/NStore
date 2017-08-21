using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;

namespace NStore.Streams
{
    public static class StreamExtensions
    {
        public static Task ReadAsync(this IReadOnlyStream stream, StreamDataProcessor fn)
        {
            return stream.ReadAsync(new LambdaSubscription(fn), 0, long.MaxValue, CancellationToken.None);
        }

        public static Task ReadAsync(this IReadOnlyStream stream, ISubscription subscription)
        {
            return stream.ReadAsync(subscription, 0, long.MaxValue, CancellationToken.None);
        }

        public static Task AppendAsync(this IStream stream, object payload)
        {
            return stream.AppendAsync(payload, null, CancellationToken.None);
        }

        public static Task AppendAsync(this IStream stream, object payload, string operationId)
        {
            return stream.AppendAsync(payload, operationId, CancellationToken.None);
        }

        public static Task DeleteAsync(this IStream stream)
        {
            return stream.DeleteAsync(CancellationToken.None);
        }
    }
}