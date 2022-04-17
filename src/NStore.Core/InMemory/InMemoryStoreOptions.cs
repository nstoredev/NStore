using System;

namespace NStore.Core.InMemory
{
    public class InMemoryStoreOptions
    {
        public InMemoryStoreOptions() : this(null, null)
        {

        }

        public InMemoryStoreOptions(Func<object, object> cloneFunc, INetworkSimulator networkSimulator)
        {
            CloneFunc = cloneFunc;
            NetworkSimulator = networkSimulator; 
        }

        public Func<object, object> CloneFunc { get; set; }

        public INetworkSimulator NetworkSimulator { get; set; }
    }
}
