using System;
using NStore.Aggregates;
using Xunit;

namespace NStore.Tests.AggregatesTests
{
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
        public void calling_init_more_than_once_should_throw_()
        {
            var ticket = new Ticket();
            ticket.Init("abc");

            var ex = Assert.Throws<AggregateAlreadyInitializedException>(() => ticket.Init("bce"));
            Assert.Equal("abc", ex.AggregateId);
            Assert.Equal(typeof(Ticket), ex.AggregateType);
        }

        [Theory()]
        [InlineData(null)]
        [InlineData("")]
        public void cannot_init_with_invalid_id(string id)
        {
            var ticket = new Ticket();
            Assert.Throws<ArgumentNullException>(() => ticket.Init(id));
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
            Ticket ticket = TicketTestFactory.ForTest();
            var persister = (IAggregatePersister) ticket;
            var commit = new Commit(1, new TicketSold());
            persister.AppendCommit(commit);

            Assert.True(ticket.IsInitialized);
            Assert.Equal(1, ticket.Version);
            Assert.Empty(ticket.UncommittedEvents);
        }

        [Fact]
        public void raising_event_should_not_increase_version()
        {
            Ticket ticket = TicketTestFactory.ForTest();

            ticket.Sale();
            Assert.Equal(0, ticket.Version);

            Assert.Equal(1, ticket.UncommittedEvents.Count);
            Assert.IsType<TicketSold>(ticket.UncommittedEvents[0]);
            Assert.True(ticket.ExposedStateForTest.HasBeenSold);
        }

        [Fact]
        public void aggregate_without_uncommitted_events_should_build_an_empty_commit()
        {
            var ticket = TicketTestFactory.ForTest();
            var persister = (IAggregatePersister)ticket;
            var commit = persister.BuildCommit();

            Assert.NotNull(commit);
            Assert.True(commit.IsEmpty);
        }

        [Fact]
        public void persister_should_create_commit_with_uncommitted_events()
        {
            var ticket = TicketTestFactory.Sold();
            var persister = (IAggregatePersister)ticket;
            var commit = persister.BuildCommit();

            Assert.NotNull(commit);
            Assert.False(commit.IsEmpty);
            Assert.Equal(1, commit.Version);
        }

        [Fact]
        public void persister_should_create_commit_only_with_uncommitted_events()
        {
            var ticket = TicketTestFactory.ForTest();
            var persister = (IAggregatePersister)ticket;

            var commit = new Commit(1, new TicketSold());
            persister.AppendCommit(commit);

            ticket.Refund();

            commit = persister.BuildCommit();

            Assert.NotNull(commit);
            Assert.False(commit.IsEmpty);
            Assert.Equal(2, commit.Version);
            Assert.Equal(1, commit.Events.Length);
            Assert.IsType<TicketRefunded>(commit.Events[0]);
        }


    }
}