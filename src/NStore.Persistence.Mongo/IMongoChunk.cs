namespace NStore.Persistence.Mongo
{
    public interface IMongoChunk : IChunk
    {
        void ReplacePayload(object payload);
        void RewriteIndex(long newIndex);
        void RewritePosition(long newPosition);

        void Init(long id, string partitionId, long index, object payload, string operationId);
    }
}