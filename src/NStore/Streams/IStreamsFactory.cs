namespace NStore.Streams
{
    public interface IStreamsFactory
    {
        IStream Open(string streamId);
        IStream OpenOptimisticConcurrency(string streamId);
        IStream OpenReadOnly(string streamId);
    }
}