using NStore.Aggregates;
using Xunit;

namespace NStore.Tests.AggregatesTests
{
    public static class TicketFactory
    {
        public static Ticket ForTest(string id = "Ticket_1", bool init = true)
        {
            var ticket = new Ticket();

            if (init)
                ticket.Init(id);

            return ticket;
        }
    }

    public class AggregateTests
    {
        [Fact]
        public void new_aggregate_should_not_be_itialized()
        {
            var ticket = new Ticket();

            Assert.False(ticket.IsInitialized);
            Assert.Equal(0, ticket.Version);
            Assert.Empty(ticket.UncommittedEvents);
            Assert.Null(ticket.ExposedStateForTest);
        }

        [Fact]
        public void init_without_params_should_create_default_state()
        {
            var ticket = new Ticket();
            ticket.Init("new_ticket");

            Assert.NotNull(ticket.ExposedStateForTest);
            Assert.Equal("new_ticket", ticket.Id);
        }

        [Fact]
        public void append_should_increase_version()
        {
            Ticket ticket = TicketFactory.ForTest();
            var persister = (IAggregatePersister) ticket;

            persister.Append(1, new object[] {new TicketSold()});

            Assert.True(ticket.IsInitialized);
            Assert.Equal(1, ticket.Version);
            Assert.Empty(ticket.UncommittedEvents);
        }

        [Fact]
        public void raising_event_should_not_increase_version()
        {
            Ticket ticket = TicketFactory.ForTest();

            ticket.Sale();
            Assert.Equal(0, ticket.Version);

            Assert.Equal(1, ticket.UncommittedEvents.Count);
            Assert.IsType<TicketSold>(ticket.UncommittedEvents[0]);
            Assert.True(ticket.ExposedStateForTest.HasBeenSold);
        }
    }
}