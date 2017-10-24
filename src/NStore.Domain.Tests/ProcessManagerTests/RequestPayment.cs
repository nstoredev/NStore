namespace NStore.Domain.Tests.ProcessManagerTests
{
    public class RequestPayment
    {
        public string OrderId { get; private set; }

        public RequestPayment(string orderId)
        {
            OrderId = orderId;
        }
    }

    public class CancelOrder
    {
        public string OrderId { get; private set; }
        public string Message { get; private set; }
        public CancelOrder(string orderId, string message)
        {
            OrderId = orderId;
            Message = message;
        }
    }
}