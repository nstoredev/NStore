using System;
using System.Collections;
#pragma warning disable S1172 // Unused method parameters should be removed
#pragma warning disable S1144 // Unused private types or members should be removed
namespace NStore.Domain.Tests.ProcessManagerTests
{
    public class CheckoutState
    {
        public bool PaymentReceived { get; private set; }
        public bool Shipped { get; private set; }

        private IEnumerable On(OrderPlaced e)
        {
            yield return new RequestPayment(e.OrderId)
                .AndSignalTimeoutAfter(TimeSpan.FromHours(1))
                .ToSelf();
            yield return new SendPurchaseConfirmation(e.OrderId);
        }

        private ShipOrder On(PaymentReceived e)
        {
            this.PaymentReceived = true;
            return new ShipOrder(e.OrderId);
        }

        private void On(OrderShipped e)
        {
            this.Shipped = true;
        }

        private void On(MessageAndTimeout<RequestPayment> e)
        {
            // todo
        }

        private ScheduledAt<CheckPaymentReceived> On(PaymentRequested e)
        {
            this.PaymentReceived = true;
            return new CheckPaymentReceived(e.OrderId).HappensAt(e.TimeStamp.AddDays(1));
        }

        private object On(CheckPaymentReceived e)
        {
            if (!this.PaymentReceived)
            {
                return new CancelOrder(e.OrderId, "Payment timeout");
            }

            return null;
        }
    }
}
#pragma warning restore S1144 // Unused private types or members should be removed
#pragma warning restore S1172 // Unused method parameters should be removed
