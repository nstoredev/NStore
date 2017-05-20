using NStore.InMemory;
using NStore.Raw;
using NStore.SnapshotStore;

namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        public IRawStore Create()
        {
            var store = new InMemoryRawStore(cloneFunc:Clone);
            return store;
        }

        public void Clear()
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