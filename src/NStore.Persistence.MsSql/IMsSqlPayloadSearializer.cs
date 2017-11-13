namespace NStore.Persistence.MsSql
{
    public interface IMsSqlPayloadSearializer
    {
        byte[] Serialize(object payload);
        object Deserialize(byte[] serialized);
    }
}