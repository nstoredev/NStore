namespace NStore.Persistence.MsSql
{
    public interface IMsSqlPayloadSearializer
    {
        byte[] Serialize(object payload, out string serializerInfo);
        object Deserialize(byte[] serialized, string serializerInfo);
    }
}