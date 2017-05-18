using System;

namespace NStore.SnapshotStore
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