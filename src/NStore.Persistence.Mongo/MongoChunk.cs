using MongoDB.Bson.Serialization.Attributes;

namespace NStore.Persistence.Mongo
{
    public class MongoChunk : IMongoChunk
    {
        [BsonId]
        public long Position { get; private set; }

        public string PartitionId { get; private set; }
        public long Index { get; private set; }
        public object Payload { get; private set; }
        public string OperationId { get; private set; }

        public void ReplacePayload(object payload)
        {
            this.Payload = payload;
        }

        public void RewriteIndex(long newIndex)
        {
            this.Index = newIndex;
        }

        public void RewritePosition(long newPosition)
        {
            this.Position = newPosition;
        }

        public void RewriteOperationId(string id)
        {
            this.OperationId = id;
        }

        /// <summary>
        /// Complete initialization of a chunk, this is interesting because it will give the opportunity
        /// to use pooling of chunk instances.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="partitionId"></param>
        /// <param name="index"></param>
        /// <param name="payload"></param>
        /// <param name="operationId"></param>
        public virtual void Init(
            long id,
            string partitionId,
            long index,
            object payload,
            string operationId)
        {
            Position = id;
            PartitionId = partitionId;
            Index = index;
            Payload = payload;
            OperationId = operationId;
        }
    }
}