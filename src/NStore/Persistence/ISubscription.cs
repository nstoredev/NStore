using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public interface ISubscription
    {
        Task OnStartAsync(long position);
        Task<bool> OnNextAsync(IChunk data);
        Task CompletedAsync(long position);
        Task StoppedAsync(long position);
        Task OnErrorAsync(long position, Exception ex);
    }

    public delegate Task<bool> StreamDataProcessor(IChunk data);
}