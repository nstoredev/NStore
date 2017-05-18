namespace NStore.Raw
{
    public interface IPartitionObserver
    {
        ScanCallbackResult Observe(
            long partitionIndex, 
            object payload
        );
    }
}