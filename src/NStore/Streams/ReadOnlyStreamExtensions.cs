using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;

namespace NStore.Streams
{
    public static class ReadOnlyStreamExtensions
    {
        public static Task ReadAsync(this IReadOnlyStream stream, StreamDataProcessor fn)
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

        public static Task ReadAsync(this IReadOnlyStream stream, ISubscription subscription, long fromIndexInclusive, long toIndexInclusive)
        {
            return stream.ReadAsync(subscription, fromIndexInclusive, toIndexInclusive, CancellationToken.None);
        }
    }
}