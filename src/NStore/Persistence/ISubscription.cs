using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public interface IChunk
    {
        long Position { get; }
        string PartitionId { get; }
        long Index { get; }
        object Payload { get; }
    }

    public interface ISubscription
    {
        Task<bool> OnNext(IChunk data);
        Task Completed();
        Task OnError(Exception ex);
    }

    public delegate Task<bool> StreamDataProcessor(IChunk data);
}