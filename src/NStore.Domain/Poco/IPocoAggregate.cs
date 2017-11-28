namespace NStore.Domain.Poco
{
    public interface IPocoAggregate
    {
        void Do(object command);
    }
}