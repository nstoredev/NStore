using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;

namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private const string TestSuitePrefix = "Memory";

        protected internal IPersistence Create(bool dropOnInit)
        {
            var options = new InMemoryPersistenceOptions
            {
                CloneFunc = Clone,
                UseSharedPartitionCollection = true,
                DropSharedPartitionCollectionOnInit = dropOnInit
            };
            return new InMemoryPersistence(options);
        }

        protected internal void Clear()
        {
        }

        private static object Clone(object source)
        {
            if (source == null)
                return null;

            if (source is SnapshotInfo si)
            {
                return new SnapshotInfo(
                    si.SourceId,
                    si.SourceVersion,
                    new State((State)si.Payload),
                    si.SchemaVersion
                );
            }

            return source;
        }
    }
}