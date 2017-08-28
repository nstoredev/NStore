namespace NStore.Aggregates
{
    public interface IInvariantsChecker
    {
        InvariantsCheckResult CheckInvariants();
    }
}