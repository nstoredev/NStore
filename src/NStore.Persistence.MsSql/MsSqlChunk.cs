using NStore.Core.Persistence;

namespace NStore.Persistence.MsSql
{
    public sealed class MsSqlChunk : IChunk
    {
        public long Position { get; set; }
        public string PartitionId { get; set; }
        public long Index { get; set; }
        public string OperationId { get; set; }
        public string SerializerInfo { get; set; }
        public object Payload { get; set; }
    }
}