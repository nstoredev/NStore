using Xunit;

namespace NStore.Tests.AggregatesTests
{
    public static class TicketFactory
    {
        public static Ticket ForTest(bool init = true)
        {
            var ticket = new Ticket();

            if (init)
                ticket.Init();

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
            ticket.Init();

            Assert.NotNull(ticket.ExposedStateForTest);
        }

        [Fact]
        public void append_should_increase_version()
        {
            Ticket ticket = TicketFactory.ForTest();

            ticket.Append(new TicketSold());

            Assert.True(ticket.IsInitialized);
            Assert.Equal(1, ticket.Version);
            Assert.Empty(ticket.UncommittedEvents);
        }

        [Fact]
        public void raising_event_should_increate_version()
        {
            Ticket ticket = TicketFactory.ForTest();

            ticket.Sale();
            Assert.Equal(1, ticket.Version);
            Assert.Equal(1, ticket.UncommittedEvents.Count);
            Assert.IsType<TicketSold>(ticket.UncommittedEvents[0]);
            Assert.True(ticket.ExposedStateForTest.HasBeenSold);
        }
    }
}