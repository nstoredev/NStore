using System;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;

namespace NStore.Domain.Experimental
{
    public class DomainBuilder
    {
        private Func<IPersistence> _persistenceFactory;
        private Func<IAggregateFactory> _aggregateFactoryFactory;
        private Func<ISnapshotStore> _snapshotStoreFactory = () => null;
        private ChunkProcessor _chunkProcessor;

        public DomainRuntime Build()
        {
            return new DomainRuntime(
                _persistenceFactory(), 
                _aggregateFactoryFactory(),
                _snapshotStoreFactory(),
                _chunkProcessor
            );
        }

        public DomainBuilder PersistOn(Func<IPersistence> persistenceFactory)
        {
            _persistenceFactory = persistenceFactory;
            return this;
        }        
        
        public DomainBuilder WithSnapshotsOn(Func<ISnapshotStore> snapshotStoreFactory)
        {
            _snapshotStoreFactory = snapshotStoreFactory;
            return this;
        }

        public DomainBuilder CreateAggregatesWith(Func<IAggregateFactory> aggregateFactoryFactory)
        {
            this._aggregateFactoryFactory = aggregateFactoryFactory;
            return this;
        }

        public DomainBuilder BroadcastTo(ChunkProcessor processor)
        {
            _chunkProcessor = processor;
            return this;
        }
    }
}