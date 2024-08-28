using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public class NullSubscription : ISubscription
    {
        public static readonly NullSubscription Instance = new NullSubscription();

        private NullSubscription()
        {
        }

        public Task<bool> OnNextAsync(IChunk chunk, CancellationToken cancellationToken)
        {
            // continue
            return Task.FromResult(true);
		}

        public Task OnStartAsync(long indexOrPosition, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task CompletedAsync(long indexOrPosition, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task StoppedAsync(long indexOrPosition, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(long indexOrPosition, Exception ex, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
