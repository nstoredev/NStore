using NStore.Persistence;

namespace NStore.InMemory
{
    internal class Chunk : IChunk
    {
        public long Position { get; set; }
        public string PartitionId { get; set; }
        public long Index { get; set; }
        public object Payload { get; set; }
        public string OpId { get; set; }
        public bool Deleted { get; set; }
    }
}