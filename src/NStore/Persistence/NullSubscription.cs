using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
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

        public Task OnStartAsync(long position)
        {
            return Task.CompletedTask;
        }

        public Task CompletedAsync(long position)
        {
            return Task.CompletedTask;
        }

        public Task StoppedAsync(long position)
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(long position, Exception ex )
        {
            return Task.CompletedTask;
        }
    }
}