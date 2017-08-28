namespace NStore.Domain
{
    public interface IAggregate
    {
        string Id { get; }
        long Version { get; }

        bool IsInitialized { get; }
        bool IsDirty { get; }
        bool IsNew { get; }
        void Init(string id);
    }
}