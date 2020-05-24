namespace NStore.Core.Streams
{
    public interface IStreamsFactory
    {
        IStream Open(string streamId);
        IStream OpenOptimisticConcurrency(string streamId);
        IStream OpenReadOnly(string streamId);

        IRandomAccessStream OpenRandomAccess(string streamId);
    }
}