using NStore.InMemory;
using NStore.Persistence;
using NStore.SnapshotStore;

namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private const string TestSuitePrefix = "Memory";

        private IPersistence Create()
        {
            var store = new InMemoryPersistence(cloneFunc:Clone);
            return store;
        }

        private void Clear()
        {
        }
        
        private static object Clone(object source)
        {
            if (source == null)
                return null;

            if (source is SnapshotInfo si)
            {
                return new SnapshotInfo(
                    si.AggregateId,
                    si.AggregateVersion,
                    new State((State) si.Data),
                    si.SnapshotVersion
                );
            }

            return source;
        }
    }
}