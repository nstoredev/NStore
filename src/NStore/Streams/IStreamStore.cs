namespace NStore.Streams
{
    public interface IStreamStore
    {
        IStream Open(string streamId);
        IStream OpenOptimisticConcurrency(string streamId);
        IStream OpenReadOnly(string streamId);
    }
}