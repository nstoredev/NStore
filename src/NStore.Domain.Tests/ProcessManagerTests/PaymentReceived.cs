namespace NStore.Domain.Tests.ProcessManagerTests
{
    public class PaymentReceived
    {
        public string OrderId { get; }

        public PaymentReceived(string orderId)
        {
            OrderId = orderId;
        }
    }
}