namespace NStore.Raw
{
    public interface IStoreObserver
    {
        ScanCallbackResult Observe(
            long storeIndex,
            string streamId,
            long partitionIndex,
            object payload
        );
    }
}