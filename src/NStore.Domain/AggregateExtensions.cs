namespace NStore.Domain
{
    public static class AggregateExtensions
    {
        public static bool IsNew(this IAggregate aggregate) =>
            aggregate.IsInitialized && aggregate.Version == 0;
    }
}