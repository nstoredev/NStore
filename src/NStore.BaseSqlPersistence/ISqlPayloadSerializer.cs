namespace NStore.BaseSqlPersistence
{
    public interface ISqlPayloadSerializer
    {
        byte[] Serialize(object payload, out string serializerInfo);
        object Deserialize(byte[] serialized, string serializerInfo);
    }
}