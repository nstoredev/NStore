namespace NStore.Aggregates
{
    public interface IAggregate
    {
        int Version { get; }

        bool IsInitialized { get; }
        void Append(object @event);
        void Init(int version = 0, object @state = null);
    }

    public static class AggregateExtensions
    {
        public static bool IsNew(this IAggregate aggregate) =>
            aggregate.IsInitialized && aggregate.Version == 0;
    }
}