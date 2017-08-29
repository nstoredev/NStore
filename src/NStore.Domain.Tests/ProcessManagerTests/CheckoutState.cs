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
            yield return new RequestPayment(e.OrderId);
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
    }
}
#pragma warning restore S1144 // Unused private types or members should be removed
#pragma warning restore S1172 // Unused method parameters should be removed
