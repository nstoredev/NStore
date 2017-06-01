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
        ScanAction Consume(
            long partitionIndex, 
            object payload
        );

        void Completed();
        void OnError(Exception ex);
    }
}