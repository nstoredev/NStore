using System;
using System.Collections.Generic;
using System.Text;

namespace NStore.Aggregates
{
    public class AggregateRestoreException : Exception
    {
        public long RestoreVersion { get; }
        public long ExpectedVersion { get; }

        public AggregateRestoreException(long expectedVersion, long restoreVersion)
        {
            ExpectedVersion = expectedVersion;
            RestoreVersion = restoreVersion;
        }
    }
}
