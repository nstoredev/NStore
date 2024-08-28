using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public static class SubscriptionExtension
    {
        public static Task OnStartAsync(this ISubscription subscription, long indexOrPosition)
        {
            return subscription.OnStartAsync(indexOrPosition, CancellationToken.None);
        }

        public static Task<bool> OnNextAsync(this ISubscription subscription, IChunk chunk)
        {
            return subscription.OnNextAsync(chunk, CancellationToken.None);
        }

        public static Task CompletedAsync(this ISubscription subscription, long indexOrPosition)
        {
            return subscription.CompletedAsync(indexOrPosition, CancellationToken.None);
        }

        public static Task StoppedAsync(this ISubscription subscription, long indexOrPosition)
        {
            return subscription.StoppedAsync(indexOrPosition, CancellationToken.None);
        }

        public static Task OnErrorAsync(this ISubscription subscription, long indexOrPosition, System.Exception ex)
        {
            return subscription.OnErrorAsync(indexOrPosition, ex, CancellationToken.None);
        }
    }
}
