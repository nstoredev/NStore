namespace NStore.Aggregates
{
    public interface IAggregate
    {
        int Version { get; }

        bool Initialized { get; }
        void Append(object @event);
    }
}