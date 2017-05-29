using System;
using NStore.Aggregates;
using NStore.InMemory;
using NStore.Persistence;
using NStore.SnapshotStore;
using NStore.Streams;
using Xunit;

// ReSharper disable InconsistentNaming

namespace NStore.Tests.AggregatesTests
{
    public abstract class BaseRepositoryTest
    {
        protected IStreamStore Streams { get; }
        protected IPersistence Raw { get; }
        private IAggregateFactory AggregateFactory { get; }
        protected ISnapshotStore Snapshots { get; set; }
        private IRepository _repository;
        protected IRepository Repository => _repository ?? (_repository = CreateRepository());

        protected BaseRepositoryTest()
        {
            Raw = new InMemoryPersistence();

            Streams = new StreamStore(Raw);
            AggregateFactory = new DefaultAggregateFactory();
        }

        protected IRepository CreateRepository()
        {
            return new Repository(
                AggregateFactory,
                Streams,
                Snapshots
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
            var tape = new PartitionRecorder();
            await stream.Read(tape,0);

            Assert.Equal(1, tape.Length);
            Assert.IsType<Changeset>(tape[0]);
        }

        [Fact]
        public async void save_can_add_custom_info_in_headers()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            ticket.Sale();
            await Repository.Save(ticket, "op_1", h => h.Add("a", "b"));

            // load stream
            var stream = Streams.Open("Ticket_1");
            var tape = new PartitionRecorder();
            await stream.Read(tape,0);

            var changeSet = (Changeset)tape[0];
            Assert.True(changeSet.Headers.ContainsKey("a"));
            Assert.Equal("b", changeSet.Headers["a"]);
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
            var tape = new PartitionRecorder();
            await stream.Read(tape,0);
            Assert.Equal(1, tape.Length);
        }
    }

    public class with_populated_stream : BaseRepositoryTest
    {
        public with_populated_stream()
        {
            Raw.PersistAsync("Ticket_1", 1, new Changeset(1, new TicketSold())).Wait();
            Raw.PersistAsync("Ticket_1", 2, new Changeset(2, new TicketRefunded())).Wait();
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
        public async void loading_aggregate_twice_from_repository_should_return_different_istance()
        {
            var ticket1 = await Repository.GetById<Ticket>("Ticket_1");
            var ticket2 = await Repository.GetById<Ticket>("Ticket_1");

            Assert.NotSame(ticket1, ticket2);
        }

        [Fact]
        public async void loading_aggregate_twice_from_identity_map_should_return_same_istance()
        {
            var repository = new  IdentityMapRepositoryDecorator(Repository);
            var ticket1 = await repository.GetById<Ticket>("Ticket_1");
            var ticket2 = await repository.GetById<Ticket>("Ticket_1");

            Assert.Same(ticket1, ticket2);
        }

        [Fact]
        public async void loading_aggregate_twice_at_different_version_from_repository_should_return_different_istances()
        {
            var ticket1 = await Repository.GetById<Ticket>("Ticket_1", 1);
            var ticket2 = await Repository.GetById<Ticket>("Ticket_1", 2);

            Assert.NotSame(ticket1, ticket2);
        }

        [Fact]
        public async void loading_an_old_version_should_return_different_instance()
        {
            var ticket_at_v2 = await Repository.GetById<Ticket>("Ticket_1", 2);
            var ticket_latest = await Repository.GetById<Ticket>("Ticket_1");

            Assert.NotSame(ticket_at_v2, ticket_latest);
            Assert.Equal(ticket_at_v2.Version, ticket_latest.Version);
        }

        [Fact]
        public async void cannot_save_a_partially_loaded_aggregate()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1", 1);
            ticket.Refund();
            var ex = await Assert.ThrowsAsync<AggregateReadOnlyException>(() =>
                Repository.Save(ticket, Guid.NewGuid().ToString())
            );
        }
    }

    public class with_snapshots : BaseRepositoryTest
    {
        public with_snapshots()
        {
            Snapshots = new DefaultSnapshotStore(new InMemoryPersistence());

            Raw.PersistAsync("Ticket_1", 1, new Changeset(1, new TicketSold())).Wait();
            Raw.PersistAsync("Ticket_1", 2, new Changeset(2, new TicketRefunded())).Wait();
        }

        [Fact]
        public async void can_load_without_snapshot_present()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1", 2);
            Assert.Equal(2, ticket.Version);
        }

        [Fact]
        public async void saving_should_create_snapshot()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            ticket.Refund();
            await Repository.Save(ticket, "save_snap");

            var snapshot = await Snapshots.Get("Ticket_1", int.MaxValue);
            Assert.NotNull(snapshot);
            Assert.Equal(3, snapshot.AggregateVersion);
            Assert.NotNull(snapshot.Data);
            Assert.Equal(1, snapshot.SnapshotVersion);
        }

        [Fact]
        public async void saving_new_aggregate_should_create_snapshot()
        {
            var ticket = await Repository.GetById<Ticket>("new_ticket");
            ticket.Sale();
            await Repository.Save(ticket, "save_snap");

            var snapshot = await Snapshots.Get("new_ticket", int.MaxValue);
            Assert.NotNull(snapshot);
            Assert.Equal(1, snapshot.AggregateVersion);
            Assert.NotNull(snapshot.Data);
            Assert.Equal(1, snapshot.SnapshotVersion);
        }
    }

    public class with_snapshot_only : BaseRepositoryTest
    {
        public with_snapshot_only()
        {
            Snapshots = new DefaultSnapshotStore(new InMemoryPersistence());
        }

        [Fact]
        public async void with_snapshot_but_without_stream_should_throw_stale_aggregate_exception()
        {
            var ticketState = new TicketState();
            var snapshot = new SnapshotInfo("Ticket_1", 2, ticketState, 1);
            await Snapshots.Add("Ticket_1", snapshot);

            var ex = await Assert.ThrowsAsync<StaleSnapshotException>(() =>
                Repository.GetById<Ticket>("Ticket_1")
            );

            Assert.Equal("Ticket_1", ex.AggregateId);
            Assert.Equal(2, ex.AggregateVersion);
        }
    }
}