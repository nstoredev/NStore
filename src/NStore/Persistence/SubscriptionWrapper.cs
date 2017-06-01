using System;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class SubscriptionWrapper : ISubscription
    {
        private readonly ISubscription _wrapped;

        public SubscriptionWrapper(ISubscription wrapped)
        {
            _wrapped = wrapped;
        }

        public Action<IChunk> BeforeOnNext { get; set; }

        public async Task<bool> OnNext(IChunk data)
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