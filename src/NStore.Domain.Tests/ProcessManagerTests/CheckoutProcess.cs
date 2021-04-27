namespace NStore.Domain.Tests.ProcessManagerTests
{
    public class CheckoutProcess : ProcessManager<CheckoutState>
    {
        public CheckoutProcess()
        {
        }

        public CheckoutState ExposeStateForTest => State;
    }
}