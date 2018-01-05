using System;
using System.Collections.Concurrent;

namespace NStore.Core.InMemory
{
    public class InMemoryPersistenceOptions
    {
        public Func<object, object> CloneFunc { get; set; }

        public INetworkSimulator NetworkSimulator { get; set; }
    }
}
