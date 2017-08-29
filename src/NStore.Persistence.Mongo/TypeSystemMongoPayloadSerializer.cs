namespace NStore.Persistence.Mongo
{
    public class TypeSystemMongoPayloadSerializer : IMongoPayloadSerializer
    {
        public object Deserialize(object payload)
        {
            return payload;
        }

        public object Serialize(object payload)
        {
            return payload;
        }
    }
}