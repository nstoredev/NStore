using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public class SubscriptionWrapper : ISubscription
    {
        private readonly ISubscription _wrapped;

        public SubscriptionWrapper(ISubscription wrapped)
        {
            _wrapped = wrapped;
            ChunkFilter = c => true;
        }

        public Action<IChunk> BeforeOnNext { get; set; }
        public Func<IChunk, bool> ChunkFilter { get; set; }

        public async Task<bool> OnNextAsync(IChunk chunk, CancellationToken cancellationToken)
        {
            if (ChunkFilter(chunk))
            {
                BeforeOnNext?.Invoke(chunk);
                return await _wrapped.OnNextAsync(chunk, cancellationToken).ConfigureAwait(false);
            }

            return true;
        }

        public async Task CompletedAsync(long indexOrPosition, CancellationToken cancellationToken)
        {
            await _wrapped.CompletedAsync(indexOrPosition, cancellationToken).ConfigureAwait(false);
        }

        public async Task StoppedAsync(long indexOrPosition, CancellationToken cancellationToken)
        {
            await _wrapped.StoppedAsync(indexOrPosition, cancellationToken).ConfigureAwait(false);
        }

        public async Task OnStartAsync(long indexOrPosition, CancellationToken cancellationToken)
        {
            await _wrapped.OnStartAsync(indexOrPosition, cancellationToken).ConfigureAwait(false);
        }

        public async Task OnErrorAsync(long indexOrPosition, Exception ex, CancellationToken cancellationToken)
        {
            await _wrapped.OnErrorAsync(indexOrPosition, ex, cancellationToken).ConfigureAwait(false);
        }
    }
}
