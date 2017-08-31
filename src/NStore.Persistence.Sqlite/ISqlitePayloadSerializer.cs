namespace NStore.Persistence.Sqlite
{
    public interface ISqlitePayloadSerializer
    {
        string Serialize(object payload);
        object Deserialize(string serialized);
    }
}