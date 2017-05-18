namespace NStore.Persistence.Mongo
{
    public interface ISerializer
    {
        object Serialize(object input);
        object Deserialize(object input);
    }
}