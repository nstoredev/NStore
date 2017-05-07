namespace NStore.Aggregates
{
    public interface IAggregateFactory
    {
        T Create<T>() where T : IAggregate;
    }
}