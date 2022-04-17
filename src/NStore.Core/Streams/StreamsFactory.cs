using System;
using NStore.Core.Persistence;

namespace NStore.Core.Streams
{
    public class StreamsFactory : IStreamsFactory
    {
        private readonly IPersistence _store;

        public StreamsFactory(IPersistence persistence)
        {
            _store = persistence ?? throw new ArgumentNullException(nameof(persistence));
        }

        public IRandomAccessStream OpenRandomAccess(string streamId)
        {
            return Open(streamId) as IRandomAccessStream;
        }

        public IStream Open(string streamId)
        {
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));
            return new Stream(streamId, _store);
        }

        public IStream OpenOptimisticConcurrency(string streamId)
        {
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));
            return new OptimisticConcurrencyStream(streamId, _store);
        }

        public IStream OpenReadOnly(string streamId)
        {
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));
            return new ReadOnlyStream(streamId, _store);
        }
    }
}