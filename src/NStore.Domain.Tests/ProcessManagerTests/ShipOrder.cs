namespace NStore.Domain.Tests.ProcessManagerTests
{
    public class ShipOrder
    {
        public string OrderId { get; }

        public ShipOrder(string orderId)
        {
            OrderId = orderId;
        }
    }

    public class CheckPaymentReceived
    {
        public string OrderId { get; private set; }

        public CheckPaymentReceived(string orderId)
        {
            OrderId = orderId;
        }
    }
}
