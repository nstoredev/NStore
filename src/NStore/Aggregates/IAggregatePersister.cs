namespace NStore.Aggregates
{
    public interface IAggregatePersister
    {
        void AppendCommit(Commit commit);
        Commit BuildCommit();
        void CommitPersisted(Commit commit);
    }
}