using System;

namespace NStore.Domain
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
