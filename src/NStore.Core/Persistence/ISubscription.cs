using System;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public interface ISubscription
    {
        Task OnStartAsync(long indexOrPosition);
        Task<bool> OnNextAsync(IChunk chunk);
        Task CompletedAsync(long indexOrPosition);
        Task StoppedAsync(long indexOrPosition);
        Task OnErrorAsync(long indexOrPosition, Exception ex);
    }

    public delegate Task<bool> ChunkProcessor(IChunk chunk);
}