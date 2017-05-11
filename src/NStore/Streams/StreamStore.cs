using NStore.Raw;

namespace NStore.Streams
{
    public interface IStreamStore
    {
        IStream Open(string streamId);
        IStream OpenOptimisticConcurrency(string streamId);
        IStream OpenReadOnly(string streamId);
    }

    public class StreamStore : IStreamStore
    {
        private readonly IRawStore _raw;

        public StreamStore(IRawStore raw)
        {
            _raw = raw;
        }

        public IStream Open(string streamId)
        {
            return new Stream(streamId, _raw);
        }

        public IStream OpenOptimisticConcurrency(string streamId)
        {
            return new OptimisticConcurrencyStream(streamId, _raw);
        }

        public IStream OpenReadOnly(string streamId)
        {
            return new ReadOnlyStream(streamId, _raw);
        }
    }
}