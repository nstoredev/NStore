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

        public async Task CompletedAsync(long indexOrPosition)
        {
            await _wrapped.CompletedAsync(indexOrPosition).ConfigureAwait(false);
        }

        public async Task StoppedAsync(long indexOrPosition)
        {
            await _wrapped.StoppedAsync(indexOrPosition).ConfigureAwait(false);
        }

        public async Task OnStartAsync(long indexOrPosition)
        {
            await _wrapped.OnStartAsync(indexOrPosition).ConfigureAwait(false);
        }

        public async Task OnErrorAsync(long indexOrPosition, Exception ex)
        {
            await _wrapped.OnErrorAsync(indexOrPosition, ex).ConfigureAwait(false);
        }
    }
}