namespace NStore.Persistence.Mongo
{
    public interface ISerializer
    {
        object Serialize(string partitionId, object payload);
        object Deserialize(string partitionId, object payload);
    }
}