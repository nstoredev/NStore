using System;
using System.Collections;
// ReSharper disable UnusedMember.Local
#pragma warning disable S1172 // Unused method parameters should be removed
#pragma warning disable S1144 // Unused private types or members should be removed
namespace NStore.Domain.Tests.ProcessManagerTests
{
    public class CheckoutState
    {
        public bool PaymentReceived { get; private set; }
        public bool Shipped { get; private set; }

        private IEnumerable StartedBy(OrderPlaced e)
        {
            yield return new RequestPayment(e.OrderId)
                .AndSignalTimeout()
                .ToSelf()
                .After(TimeSpan.FromHours(1));

            yield return new SendPurchaseConfirmation(e.OrderId);
        }

        private ShipOrder ContinuedBy(PaymentReceived e)
        {
            this.PaymentReceived = true;
            return new ShipOrder(e.OrderId);
        }

        private object On(MessageAndTimeout<RequestPayment> e)
        {
            if (!this.PaymentReceived)
            {
                if (e.Counter < 5)
                {
                    return e.RetryTimeoutAfter(TimeSpan.FromHours(e.Counter));
                }

                return new CancelOrder(e.Message.OrderId, "Payment timed out");
            }
            return null;
        }

        private object On(PaymentRequested e)
        {
            this.PaymentReceived = true;
            return new CheckPaymentReceived(e.OrderId)
                .HappensAfter(e.TimeStamp.AddDays(1));
        }

        private object On(CheckPaymentReceived e)
        {
            if (!this.PaymentReceived)
            {
                return new CancelOrder(e.OrderId, "Payment timeout");
            }

            return null;
        }
        
        private void CompletedBy(OrderShipped e)
        {
            this.Shipped = true;
        }
    }
}
#pragma warning restore S1144 // Unused private types or members should be removed
#pragma warning restore S1172 // Unused method parameters should be removed
// ReSharper restore UnusedMember.Local
