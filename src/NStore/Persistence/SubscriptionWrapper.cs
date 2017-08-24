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

        public async Task<bool> OnNextAsync(IChunk chunk)
        {
            BeforeOnNext?.Invoke(chunk);
            return await _wrapped.OnNextAsync(chunk).ConfigureAwait(false);
        }

        public async Task CompletedAsync(long position)
        {
            await _wrapped.CompletedAsync(position).ConfigureAwait(false);
        }

        public async Task StoppedAsync(long position)
        {
            await _wrapped.StoppedAsync(position).ConfigureAwait(false);
        }

        public async Task OnStartAsync(long position)
        {
            await _wrapped.OnStartAsync(position).ConfigureAwait(false);
        }

        public async Task OnErrorAsync(long position, Exception ex)
        {
            await _wrapped.OnErrorAsync(position, ex).ConfigureAwait(false);
        }
    }
}