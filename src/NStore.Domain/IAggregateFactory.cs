namespace NStore.Domain
{
    public interface IAggregateFactory
    {
        T Create<T>() where T : IAggregate;
    }
}