namespace NStore.Aggregates
{
    public interface IAggregatePersister
    {
        void ApplyChanges(Changeset changeset);
        Changeset GetChangeSet();
        void ChangesPersisted(Changeset changeset);
    }
}