using LiteDB;
using NStore.Core.Persistence;

namespace NStore.Persistence.LiteDB
{
    public class LiteDBChunk : IChunk
    {
        [BsonId] public long Position { get; set; }

        public string PartitionId { get; set; }
        public long Index { get; set; }
        public object Payload { get; set; }
        public string OperationId { get; set; }
        public string StreamSequence { get; set; }
        public string StreamOperation { get; set; }
    }
}