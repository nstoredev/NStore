using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Core.Streams
{
    public static class ReadOnlyStreamExtensions
    {
        public static Task ReadAsync(this IReadOnlyStream stream, ChunkProcessor fn)
        {
            return stream.ReadAsync(new LambdaSubscription(fn), 0, long.MaxValue, CancellationToken.None);
        }

        public static Task ReadAsync(this IReadOnlyStream stream, ISubscription subscription)
        {
            return stream.ReadAsync(subscription, 0, long.MaxValue, CancellationToken.None);
        }

        public static Task ReadAsync(this IReadOnlyStream stream, ISubscription subscription, long fromIndexInclusive)
        {
            return stream.ReadAsync(subscription, fromIndexInclusive, long.MaxValue, CancellationToken.None);
        }

        public static Task ReadAsync(this IReadOnlyStream stream, ISubscription subscription, long fromIndexInclusive, CancellationToken cancellationToken)
        {
            return stream.ReadAsync(subscription, fromIndexInclusive, long.MaxValue, cancellationToken);
        }

        public static Task ReadAsync(this IReadOnlyStream stream, ISubscription subscription, long fromIndexInclusive, long toIndexInclusive)
        {
            return stream.ReadAsync(subscription, fromIndexInclusive, toIndexInclusive, CancellationToken.None);
        }
    }
}