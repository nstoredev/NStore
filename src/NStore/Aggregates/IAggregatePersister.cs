namespace NStore.Aggregates
{
    public interface IAggregatePersister
    {
        void AppendCommit(Commit commit);
        Commit BuildCommit();
    }
}