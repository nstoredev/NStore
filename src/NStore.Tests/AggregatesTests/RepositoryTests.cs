using NStore.Aggregates;
using NStore.InMemory;
using NStore.Streams;
using Xunit;

// ReSharper disable InconsistentNaming

namespace NStore.Tests.AggregatesTests
{
    public abstract class BaseRepositoryTest
    {
        protected InMemoryRawStore Raw { get; }
        protected IRepository Repository { get; }

        protected BaseRepositoryTest()
        {
            Raw = new InMemoryRawStore();

            Repository = new Repository(
                new DefaultAggregateFactory(),
                new StreamStore(Raw)
            );
        }
    }

    public class with_empty_stream : BaseRepositoryTest
    {
        [Fact]
        public async void can_create_new_aggregate()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");

            Assert.NotNull(ticket);
            Assert.True(ticket.IsNew());
        }
    }

    public class with_populated_stream : BaseRepositoryTest
    {
        public with_populated_stream()
        {
            Raw.PersistAsync("Ticket_1", 1, new TicketSold()).GetAwaiter().GetResult();
            Raw.PersistAsync("Ticket_1", 2, new TicketRefunded()).GetAwaiter().GetResult();
        }

        [Fact]
        public async void can_load_ticket_at_version_1()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1",1);
            Assert.True(ticket.IsInitialized);
            Assert.Equal(1, ticket.Version);
        }

        [Fact]
        public async void can_load_ticket_at_latest_version()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            Assert.True(ticket.IsInitialized);
            Assert.Equal(2, ticket.Version);
        }
    }
}