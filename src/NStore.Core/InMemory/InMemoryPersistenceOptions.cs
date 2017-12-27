using System;

namespace NStore.Core.InMemory
{
    public class InMemoryPersistenceOptions
    {
        public Func<object, object> CloneFunc { get; set; }

        public INetworkSimulator NetworkSimulator { get; set; }

        /// <summary>
        /// Force all the instances of the repositories to use the same static partition collection
        /// </summary>
        public bool UseSharedPartitionCollection { get; set; } = false;

        public bool DropSharedPartitionCollectionOnInit { get; set; } = false;
    }
}
