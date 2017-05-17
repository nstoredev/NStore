namespace NStore.Aggregates
{
    public interface IInvariantsChecker
    {
        bool CheckInvariants();
    }

    public abstract class AggregateState : SyncProjector
    {

    }
}