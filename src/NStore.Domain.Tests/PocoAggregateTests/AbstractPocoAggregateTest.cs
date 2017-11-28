namespace NStore.Domain.Tests.PocoAggregateTests
{
    public abstract class AbstractPocoAggregateTest<TState> : AbstractAggregateTest<PocoAggregate<TState>, TState> where TState : class, new()
    {
    }
}