namespace NStore.Raw
{
    public interface IStoreConsumer
    {
        ScanAction Consume(
            long storeIndex,
            string streamId,
            long partitionIndex,
            object payload
        );
    }
}