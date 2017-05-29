namespace NStore.Persistence
{
    public interface IPartitionConsumer
    {
        ScanAction Consume(
            long partitionIndex, 
            object payload
        );
    }
}