using NStore.Core.Persistence;

namespace NStore.Core.InMemory
{
    internal class MemoryChunk : IChunk
    {
        public long Position { get; set; }
        public string PartitionId { get; set; }
        public long Index { get; set; }
        public object Payload { get; set; }
        public string PayloadType { get; set; }
        public string OperationId { get; set; }
        public bool Deleted { get; set; }
    }
}