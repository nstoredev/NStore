namespace NStore.Persistence.Mongo
{
    internal class Chunk
    {
        public long Id { get; set; }
        public string PartitionId { get; set; }
        public long Index { get; set; }
        public object Payload { get; set; }
        public string OpId { get; set; }
    }
}