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

        public async Task Completed(long position)
        {
            await _wrapped.Completed(position);
        }

        public async Task Stopped(long position)
        {
            await _wrapped.Stopped(position);
        }

        public async Task OnStart(long position)
        {
            await _wrapped.OnStart(position);
        }

        public async Task OnError(long position, Exception ex)
        {
            await _wrapped.OnError(position, ex);
        }
    }
}