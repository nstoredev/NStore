namespace NStore.Domain
{
    public interface IInvariantsChecker
    {
        InvariantsCheckResult CheckInvariants();
    }
}