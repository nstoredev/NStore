using System;

namespace NStore.Domain
{
#pragma warning disable RCS1194 // Implement exception constructors.
    public class AggregateRestoreException : Exception
    {
        public long RestoreVersion { get; }
        public long ExpectedVersion { get; }

        public AggregateRestoreException(Type aggregateType, long expectedVersion, long restoreVersion)
            : base($"Unable to restore aggregate {aggregateType}, expected Changeset with version {expectedVersion} but the aggregate received wrong version {restoreVersion} instead")
        {
            ExpectedVersion = expectedVersion;
            RestoreVersion = restoreVersion;
        }
    }
#pragma warning restore RCS1194 // Implement exception constructors. 
}
