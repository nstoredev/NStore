using System;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class PartitionConsumerWrapper : IPartitionConsumer
    {
        private readonly IPartitionConsumer _wrapped;

        public PartitionConsumerWrapper(IPartitionConsumer wrapped)
        {
            _wrapped = wrapped;
        }

        public Action<IPartitionData> BeforeOnNext { get; set; }

        public async Task<bool> OnNext(IPartitionData data)
        {
            BeforeOnNext?.Invoke(data);
            return await _wrapped.OnNext(data);
        }

        public async Task Completed()
        {
            await _wrapped.Completed();
        }

        public async Task OnError(Exception ex)
        {
            await _wrapped.OnError(ex);
        }
    }
}