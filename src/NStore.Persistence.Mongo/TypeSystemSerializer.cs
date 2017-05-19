namespace NStore.Persistence.Mongo
{
    public class TypeSystemSerializer : ISerializer
    {
        public object Deserialize(string partitionId, object payload)
        {
            return payload;
        }

        public object Serialize(string partitionId, object payload)
        {
            return payload;
        }
    }
}