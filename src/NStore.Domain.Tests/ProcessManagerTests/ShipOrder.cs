using System;
using System.Collections.Generic;
using System.Text;

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
}
