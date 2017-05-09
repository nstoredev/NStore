namespace NStore.Raw
{
    public interface IConsumer
    {
        ScanCallbackResult Consume(long partitionIndex, object payload);
    }
}