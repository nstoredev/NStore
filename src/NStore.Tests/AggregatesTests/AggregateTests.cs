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

        public static Ticket Sold(string id = "Ticket_1")
        {
            var ticket = ForTest(id);
            ticket.Sale();
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
            var commit = new Commit(1, new TicketSold());
            persister.Append(commit);

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

        [Fact]
        public void aggregate_without_uncommitted_events_should_build_an_empty_commit()
        {
            var ticket = TicketFactory.ForTest();
            var persister = (IAggregatePersister)ticket;
            var commit = persister.BuildCommit();

            Assert.NotNull(commit);
            Assert.True(commit.IsEmpty);
        }

        [Fact]
        public void persister_should_create_commit_with_uncommitted_events()
        {
            var ticket = TicketFactory.Sold();
            var persister = (IAggregatePersister)ticket;
            var commit = persister.BuildCommit();

            Assert.NotNull(commit);
            Assert.False(commit.IsEmpty);
            Assert.Equal(1, commit.Version);
        }
    }
}