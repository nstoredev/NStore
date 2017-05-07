using NStore.Aggregates;
using Xunit;

namespace NStore.Tests.AggregatesTests
{
    public class RepositoryTests
    {
        private IRepository Repository { get; }

        public RepositoryTests()
        {
            Repository = new Repository(new DefaultAggregateFactory());
        }

        [Fact]
        public async void can_create_new_aggregate()
        {
            var ticket = await Repository.GetById<TicketAggregate>("Ticket_1");

            Assert.NotNull(ticket);
        }
    }
}