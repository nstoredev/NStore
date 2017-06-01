using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class NullPartitionConsumer : IPartitionConsumer
    {
        public static readonly NullPartitionConsumer Instance = new NullPartitionConsumer();

        private NullPartitionConsumer()
        {
        }

        public Task<bool> OnNext(IPartitionData data)
        {
            // continue
            return Task.FromResult(true);
		}

        public Task Completed()
        {
            return Task.CompletedTask;
        }

        public Task OnError(Exception ex )
        {
            return Task.CompletedTask;
        }
    }
}