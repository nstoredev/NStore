namespace NStore.Domain.Tests.ProcessManagerTests
{
    public class OrderShipped
    {
        public string OrderId { get; }

        public OrderShipped(string orderId)
        {
            OrderId = orderId;
        }
    }
}