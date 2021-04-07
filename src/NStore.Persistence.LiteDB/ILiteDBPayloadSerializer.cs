namespace NStore.Persistence.LiteDB
{
    public interface ILiteDBPayloadSerializer
    {
        string Serialize(object payload);
        object Deserialize(string payload);
    }
}