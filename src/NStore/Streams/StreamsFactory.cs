using System;
using NStore.Persistence;

namespace NStore.Streams
{
    public class StreamsFactory : IStreamsFactory
    {
        private readonly IPersistence _persistence;

        public StreamsFactory(IPersistence persistence)
        {
            if (persistence == null)
                throw new ArgumentNullException(nameof(persistence));

            _persistence = persistence;
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