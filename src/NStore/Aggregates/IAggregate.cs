namespace NStore.Aggregates
{
    public interface IAggregate
    {
        int Version { get; }

        bool IsInitialized { get; }
        void Append(object @event);
        void Init(object @state = null);
    }
}