using System;
using NStore.Persistence;

namespace NStore.Streams
{
    public class StreamStore : IStreamStore
    {
        private readonly IPersistence _raw;

        public StreamStore(IPersistence raw)
        {
            if (raw == null) throw new ArgumentNullException(nameof(raw));
            _raw = raw;
        }

        public IStream Open(string streamId)
        {
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));
            return new Stream(streamId, _raw);
        }

        public IStream OpenOptimisticConcurrency(string streamId)
        {
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));
            return new OptimisticConcurrencyStream(streamId, _raw);
        }

        public IStream OpenReadOnly(string streamId)
        {
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));
            return new ReadOnlyStream(streamId, _raw);
        }
    }
}