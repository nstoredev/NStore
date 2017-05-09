using NStore.Aggregates;
using Xunit;

namespace NStore.Tests.AggregatesTests
{
    public class DefaultAggregateFactoryTests
    {
        private IAggregateFactory Factory { get; }

        public DefaultAggregateFactoryTests()
        {
            Factory = new DefaultAggregateFactory();
        }

        [Fact]
        public void can_create_aggregate_with_default_ctor()
        {
            var aggregate = Factory.Create<Ticket>();

            Assert.NotNull(aggregate);
            Assert.IsType<Ticket>(aggregate);
        }

        [Fact]
        public void aggregate_must_not_be_initialized()
        {
            var aggregate = Factory.Create<Ticket>();

            Assert.False(aggregate.IsInitialized);
        }
    }
}