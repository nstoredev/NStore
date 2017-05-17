using System;
using System.Collections.Generic;
using System.Text;

namespace NStore.Aggregates
{
    public class AggregateRestoreException : Exception
    {
        public int RestoreVersion { get; }
        public int ExpectedVersion { get; }

        public AggregateRestoreException(int expectedVersion, int restoreVersion)
        {
            ExpectedVersion = expectedVersion;
            RestoreVersion = restoreVersion;
        }
    }
}
