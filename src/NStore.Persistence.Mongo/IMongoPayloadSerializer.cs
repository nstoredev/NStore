namespace NStore.Persistence.Mongo
{
    public interface IMongoPayloadSerializer
    {
        object Serialize(object payload);
        object Deserialize(object payload);
    }
}