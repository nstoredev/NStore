using System;
using NStore.Core.Persistence;

namespace NStore.Core.Streams
{
    public class StreamsFactory : IStreamsFactory
    {
        private readonly IStore _store;

        public StreamsFactory(IStore store)
        {
            _store = store ?? throw new ArgumentNullException(nameof(store));
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