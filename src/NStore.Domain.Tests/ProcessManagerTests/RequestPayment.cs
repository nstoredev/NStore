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
}