﻿using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;

namespace NStore.Persistence.Tests
{
    public partial class BaseStoreTest
    {
        private const string TestSuitePrefix = "Memory";

        protected IStore Create(bool dropOnInit)
        {
            var options = new InMemoryStoreOptions
            {
                CloneFunc = Clone,
            };
            return new InMemoryStore(options);
        }

        protected void Clear(IStore store, bool drop)
        {
            // Method intentionally left empty.
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