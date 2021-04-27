using NStore.Core.Persistence;

namespace NStore.Persistence.Mongo
{
    public interface IMongoChunk : IChunk
    {
        void ReplacePayload(object payload);
        void RewriteIndex(long newIndex);
        void RewritePosition(long newPosition);
        void RewriteOperationId(string id);

        void Init(long id, string partitionId, long index, object payload, string operationId);
    }

    public static class MongoChunkExtension
    {
        public static IMongoChunk Deserialize(this IMongoChunk chunk, IMongoPayloadSerializer serializer) 
        {
            chunk?.ReplacePayload(serializer.Deserialize(chunk.Payload));
            return chunk;
        }
    }
}