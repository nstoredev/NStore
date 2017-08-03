using MongoDB.Bson.Serialization.Attributes;

namespace NStore.Persistence.Mongo
{
    internal class Chunk : IChunk
    {
        [BsonId]
        public long Position { get; set; }

        public string PartitionId { get; set; }
        public long Index { get; set; }
        public object Payload { get; set; }
        public string OperationId { get; set; }
    }
}