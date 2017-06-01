using System;

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
        ScanAction Consume(IPartitionData data);
        void Completed();
        void OnError(Exception ex);
    }

    public delegate ScanAction ProcessPartitionData(IPartitionData data);
}