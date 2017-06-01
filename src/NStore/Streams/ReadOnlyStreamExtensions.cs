using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;

namespace NStore.Streams
{
    public static class ReadOnlyStreamExtensions
    {
        public static Task Read(this IReadOnlyStream stream, ISubscription subscription)
        {
            return stream.Read(subscription, 0, int.MaxValue, CancellationToken.None);
        }

        public static Task Read(this IReadOnlyStream stream, ISubscription subscription, int fromIndexInclusive)
        {
            return stream.Read(subscription, fromIndexInclusive, int.MaxValue, CancellationToken.None);
        }

        public static Task Read(this IReadOnlyStream stream, ISubscription subscription, int fromIndexInclusive, int toIndexInclusive)
        {
            return stream.Read(subscription, fromIndexInclusive, toIndexInclusive, CancellationToken.None);
        }
    }
}