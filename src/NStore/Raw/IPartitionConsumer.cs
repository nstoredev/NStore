namespace NStore.Raw
{
    public interface IPartitionConsumer
    {
        ScanAction Consume(
            long partitionIndex, 
            object payload
        );
    }
}