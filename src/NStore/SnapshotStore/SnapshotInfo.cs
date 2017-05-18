namespace NStore.SnapshotStore
{
    public sealed class SnapshotInfo
    {
        public SnapshotInfo(string aggregateId, int aggregateVersion, object data, int snapshotVersion)
        {
            AggregateVersion = aggregateVersion;
            Data = data;
            SnapshotVersion = snapshotVersion;
            AggregateId = aggregateId;
        }

        public int AggregateVersion { get; private set; }
        public object Data { get; private set; }
        public int SnapshotVersion { get; private set; }
        public string AggregateId { get; private set; }

        public bool IsEmpty => this.AggregateId == null ||
                               this.AggregateVersion == 0 ||
                               this.Data == null ;
    }
}