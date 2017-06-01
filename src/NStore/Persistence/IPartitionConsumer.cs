using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public interface IPartitionData
    {
        string PartitionId { get; }
        long Index { get; }
        object Payload { get; }
    }

    public interface IPersistenceData : IPartitionData
    {
        long Position { get; }
    }

    public interface IPartitionConsumer
    {
        Task<bool> OnNext(IPartitionData data);
        Task Completed();
        Task OnError(Exception ex);
    }

    public delegate Task<bool> ProcessPartitionData(IPartitionData data);
}