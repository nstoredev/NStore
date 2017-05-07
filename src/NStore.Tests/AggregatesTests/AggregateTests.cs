using Xunit;

namespace NStore.Tests.AggregatesTests
{
    public static class TicketFactory
    {
        public static Ticket ForTest()
        {
            var ticket = new Ticket();
            ticket.Init();

            return ticket;
        }
    }

    public class AggregateTests
    {
        private readonly Ticket _ticket = TicketFactory.ForTest();

        [Fact]
        public void new_aggregate_should_not_be_itialized()
        {
            Assert.False(_ticket.IsInitialized);
            Assert.Equal(0, _ticket.Version);
            Assert.Empty(_ticket.UncommittedEvents);
            Assert.NotNull(_ticket.ExposedStateForTest);
        }

        [Fact]
        public void append_should_increase_version()
        {
            _ticket.Append(new TicketSold());

            Assert.True(_ticket.IsInitialized);
            Assert.Equal(1, _ticket.Version);
            Assert.Empty(_ticket.UncommittedEvents);
        }

        [Fact]
        public void raising_event_should_increate_version()
        {
            _ticket.Sale();
            Assert.Equal(1, _ticket.Version);
            Assert.Equal(1, _ticket.UncommittedEvents.Count);
            Assert.IsType<TicketSold>(_ticket.UncommittedEvents[0]);
            Assert.True(_ticket.ExposedStateForTest.HasBeenSold);
        }
    }
}