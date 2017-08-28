using System;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public class NullSubscription : ISubscription
    {
        public static readonly NullSubscription Instance = new NullSubscription();

        private NullSubscription()
        {
        }

        public Task<bool> OnNextAsync(IChunk chunk)
        {
            // continue
            return Task.FromResult(true);
		}

        public Task OnStartAsync(long indexOrPosition)
        {
            return Task.CompletedTask;
        }

        public Task CompletedAsync(long indexOrPosition)
        {
            return Task.CompletedTask;
        }

        public Task StoppedAsync(long indexOrPosition)
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(long indexOrPosition, Exception ex )
        {
            return Task.CompletedTask;
        }
    }
}