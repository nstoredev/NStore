namespace NStore.Persistence.Mongo
{
    public class TypeSystemSerializer : ISerializer
    {
        public object Deserialize(object input)
        {
            return input;
        }

        public object Serialize(object input)
        {
            return input;
        }
    }
}