using System;
using NStore.Core.Persistence;

namespace NStore.Core.Streams
{
    public class StreamsFactory : IStreamsFactory
    {
        private readonly IPersistence _persistence;

        public StreamsFactory(IPersistence persistence)
        {
            _persistence = persistence ?? throw new ArgumentNullException(nameof(persistence));
        }

        public IStream Open(string streamId)
        {
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));
            return new Stream(streamId, _persistence);
        }

        public IStream OpenOptimisticConcurrency(string streamId)
        {
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));
            return new OptimisticConcurrencyStream(streamId, _persistence);
        }

        public IStream OpenReadOnly(string streamId)
        {
            if (streamId == null) throw new ArgumentNullException(nameof(streamId));
            return new ReadOnlyStream(streamId, _persistence);
        }
    }
}