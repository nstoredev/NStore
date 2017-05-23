using NStore.SnapshotStore;

namespace NStore.Aggregates
{
    //@@REVIEW: merge with IAggregate or move Init here
    public interface IEventSourcedAggregate
    {
        void ApplyChanges(Changeset changeset);
        Changeset GetChangeSet();
        void ChangesPersisted(Changeset changeset);
        SnapshotInfo GetSnapshot();
        bool TryRestore(SnapshotInfo snaphoInfo);
    }
}