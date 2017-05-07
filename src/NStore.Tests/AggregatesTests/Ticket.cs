using System;
using NStore.Aggregates;
using Xunit;

namespace NStore.Tests.AggregatesTests
{
    public class TicketSold
    {
    }

    public class TicketRefunded
    {
    }

    public class TicketState : AggregateState
    {
        public bool HasBeenSold { get; private set; }

        public void On(TicketSold e)
        {
            this.HasBeenSold = true;
        }
    }

    public class Ticket : Aggregate<TicketState>
    {
        public TicketState ExposedStateForTest => State;

        public void Sale()
        {
            if (State.HasBeenSold)
            {
                throw new Exception($"Ticket already sold");
            }

            Raise(new TicketSold());
        }
    }
}