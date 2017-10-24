using System;

namespace NStore.Domain.Tests.ProcessManagerTests
{
    public class PaymentReceived
    {
        public string OrderId { get; private set; }
        public DateTime TimeStamp { get; private set; }
        public PaymentReceived(string orderId, DateTime ts)
        {
            OrderId = orderId;
            TimeStamp = ts;
        }
    }

    public class PaymentRequested
    {
        public string OrderId { get; private set; }
        public DateTime TimeStamp { get; private set; }
        public PaymentRequested(string orderId, DateTime ts)
        {
            OrderId = orderId;
            TimeStamp = ts;
        }
    }
}