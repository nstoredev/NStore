namespace NStore.Snapshots
{
    public sealed class SnapshotInfo
    {
        public SnapshotInfo(
            string sourceId, 
            int sourceVersion, 
            object payload, 
            int schemaVersion)
        {
            SourceId = sourceId;
            SourceVersion = sourceVersion;
            Payload = payload;
            SchemaVersion = schemaVersion;
        }

        public int SourceVersion { get; private set; }
        public object Payload { get; private set; }
        public int SchemaVersion { get; private set; }
        public string SourceId { get; private set; }

        public bool IsEmpty => this.SourceId == null ||
                               this.SourceVersion == 0 ||
                               this.Payload == null ;
    }
}