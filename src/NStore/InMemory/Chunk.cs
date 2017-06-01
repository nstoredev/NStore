using NStore.Persistence;

namespace NStore.InMemory
{
    internal class Chunk : IPartitionData
    {
        public long Position { get; set; }
        public string PartitionId { get; set; }
        public long Index { get; set; }
        public object Payload { get; set; }
        public string OpId { get; set; }
    }
}