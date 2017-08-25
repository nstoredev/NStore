using System;

namespace NStore.Snapshots
{
    public class StaleSnapshotException : Exception
    {
        public string AggregateId { get; }
        public int AggregateVersion { get; }

        public StaleSnapshotException(string aggregateId, int aggregateVersion)
        {
            AggregateId = aggregateId;
            AggregateVersion = aggregateVersion;
        }
    }
}