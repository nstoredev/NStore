namespace NStore.Persistence.MsSql
{
    public interface IMsSqlPayloadSearializer
    {
        string Serialize(object payload);
        object Deserialize(string serialized);
    }
}