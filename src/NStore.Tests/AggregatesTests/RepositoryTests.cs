using System;
using NStore.Aggregates;
using NStore.InMemory;
using NStore.Raw;
using NStore.Streams;
using Xunit;

// ReSharper disable InconsistentNaming

namespace NStore.Tests.AggregatesTests
{
    public abstract class BaseRepositoryTest
    {
        protected IStreamStore Streams { get; }
        protected IRawStore Raw { get; }
        protected IRepository Repository { get; }

        protected BaseRepositoryTest()
        {
            Raw = new InMemoryRawStore();

            Streams = new StreamStore(Raw);
            Repository = new Repository(
                new DefaultAggregateFactory(),
                Streams
            );
        }
    }

    public class with_empty_store : BaseRepositoryTest
    {
        [Fact]
        public async void loading_an_aggregate_from_an_empty_stream_should_return_a_new_aggregate()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");

            Assert.NotNull(ticket);
            Assert.True(ticket.IsNew());
        }

        [Fact]
        public async void saving_an_aggregate_shold_persist_stream()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");

            ticket.Sale();

            await Repository.Save(ticket, "op_1");

            // load stream
            var stream = Streams.Open("Ticket_1");
            var tape = new Tape();
            await stream.Read(tape);

            Assert.Equal(1, tape.Length);
            Assert.IsType<Commit>(tape[0]);
        }
    }

    public class with_populated_stream : BaseRepositoryTest
    {
        public with_populated_stream()
        {
            Raw.PersistAsync("Ticket_1", 1, new Commit(1, new TicketSold())).GetAwaiter().GetResult();
            Raw.PersistAsync("Ticket_1", 2, new Commit(2, new TicketRefunded())).GetAwaiter().GetResult();
        }

        [Fact]
        public async void can_load_ticket_at_version_1()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1", 1);
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