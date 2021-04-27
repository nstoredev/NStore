namespace NStore.Persistence.Mongo
{
    public interface IMongoPayloadSerializer
    {
        object Serialize(object payload);
        object Deserialize(object payload);
    }

    public static class MongoChunkExtension
    {
        public static IMongoChunk ApplyDeserialization(this IMongoPayloadSerializer serializer, IMongoChunk chunk)
        {
            chunk?.ReplacePayload(serializer.Deserialize(chunk.Payload));
            return chunk;
        }
    }
}