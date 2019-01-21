using NStore.Core.InMemory;
using NStore.Core.Persistence;

namespace NStore.Tutorial.Support
{
    public static class PersistenceFactory
    {
        public static IPersistence CreateInMemory()
        {
            return new InMemoryPersistence();
        }
    }
}