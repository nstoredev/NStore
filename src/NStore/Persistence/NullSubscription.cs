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

        public Task<bool> OnNext(IChunk data)
        {
            // continue
            return Task.FromResult(true);
		}

        public Task OnStart(long position)
        {
            return Task.CompletedTask;
        }

        public Task Completed(long position)
        {
            return Task.CompletedTask;
        }

        public Task Stopped(long position)
        {
            return Task.CompletedTask;
        }

        public Task OnError(long position, Exception ex )
        {
            return Task.CompletedTask;
        }
    }
}