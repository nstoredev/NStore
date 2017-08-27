using System;

namespace NStore.Snapshots
{
    public class StaleSnapshotException : Exception
    {
        public string AggregateId { get; }
        public long AggregateVersion { get; }

        public StaleSnapshotException(string aggregateId, long aggregateVersion)
        {
            AggregateId = aggregateId;
            AggregateVersion = aggregateVersion;
        }
    }
}