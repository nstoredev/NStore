namespace NStore.Domain.Tests.ProcessManagerTests
{
    public class OrderPlaced
    {
        public OrderPlaced(string orderId)
        {
            OrderId = orderId;
        }

        public string OrderId { get; private set; }
    }

    public class SendPurchaseConfirmation
    {
        public SendPurchaseConfirmation(string orderId)
        {
            OrderId = orderId;
        }

        public string OrderId { get; private set; }
    }
}