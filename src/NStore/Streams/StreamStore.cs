using NStore.Raw;

namespace NStore.Streams
{
    public interface IStreamStore
    {
        IStream Open(string streamId);
        IOptimisticConcurrencyStream OpenOptimisticConcurrency(string streamId);
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

        public IOptimisticConcurrencyStream OpenOptimisticConcurrency(string streamId)
        {
            return new OptimisticConcurrencyStream(streamId, _raw);
        }
    }
}