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

    public class Ticket : Aggregate
    {
        public void Sale()
        {
            Raise(new TicketSold());
        }

    }
}