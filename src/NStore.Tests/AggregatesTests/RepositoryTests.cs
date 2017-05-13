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
        private IAggregateFactory AggregateFactory { get; }

        protected BaseRepositoryTest()
        {
            Raw = new InMemoryRawStore();

            Streams = new StreamStore(Raw);
            AggregateFactory = new DefaultAggregateFactory();
            Repository = CreateRepository();
        }

        protected IRepository CreateRepository()
        {
            return new Repository(
                AggregateFactory,
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
        public async void saving_an_aggregate_should_persist_stream()
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

        [Fact]
        public async void save_can_add_custom_info_in_headers()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            ticket.Sale();
            await Repository.Save(ticket, "op_1", h => h.Add("a", "b"));

            // load stream
            var stream = Streams.Open("Ticket_1");
            var tape = new Tape();
            await stream.Read(tape);

            var commit = (Commit) tape[0];
            Assert.True(commit.Headers.ContainsKey("a"));
            Assert.Equal("b", commit.Headers["a"]);
        }

        [Fact]
        public async void saving_twice_an_aggregate_should_persist_events_only_once()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            ticket.Sale();
            await Repository.Save(ticket, "op_1");
            await Repository.Save(ticket, "op_2");

            // load stream
            var stream = Streams.Open("Ticket_1");
            var tape = new Tape();
            await stream.Read(tape);
            Assert.Equal(1, tape.Length);
        }
    }

    public class with_populated_stream : BaseRepositoryTest
    {
        public with_populated_stream()
        {
            Raw.PersistAsync("Ticket_1", 1, new Commit(1, new TicketSold())).Wait();
            Raw.PersistAsync("Ticket_1", 2, new Commit(2, new TicketRefunded())).Wait();
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

        [Fact]
        public async void cannot_save_aggregate_loaded_by_another_repository()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            ticket.Refund();

            var repo2 = CreateRepository();

            var ex = await Assert.ThrowsAsync<RepositoryMismatchException>(() =>
                repo2.Save(ticket, Guid.NewGuid().ToString())
            );
        }

        [Fact]
        public async void loading_aggregate_twice_from_repository_should_return_same_istance()
        {
            var ticket1 = await Repository.GetById<Ticket>("Ticket_1");
            var ticket2 = await Repository.GetById<Ticket>("Ticket_1");

            Assert.Same(ticket1, ticket2);
        }

        [Fact]
        public async void loading_aggregate_twice_at_different_versione_from_repository_should_return_different_istances()
        {
            var ticket1 = await Repository.GetById<Ticket>("Ticket_1",1);
            var ticket2 = await Repository.GetById<Ticket>("Ticket_1",2);

            Assert.NotSame(ticket1, ticket2);
        }

        [Fact]
        public async void cannot_save_a_partially_loaded_aggregate()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1",1);
            ticket.Refund();
            var ex = await Assert.ThrowsAsync<AggregateReadOnlyException>(() =>
                Repository.Save(ticket, Guid.NewGuid().ToString())
            );
        }
    }
}