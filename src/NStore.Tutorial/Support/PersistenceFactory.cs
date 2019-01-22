using NStore.Core.InMemory;
using NStore.Core.Persistence;

namespace NStore.Tutorial.Support
{
    public static class PersistenceFactory
    {
        public static IPersistence CreateInMemory()
        {
            // Cloning function allow safe operations avoiding shared 
            // data between snapshots, aggregates, streams.
            //
            // Mimic (de)serialization of other persistence providers
            return new InMemoryPersistence( SerializationHelper.DeepClone);
        }
    }
}