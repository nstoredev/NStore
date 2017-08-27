using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Aggregates;
using NStore.InMemory;
using NStore.Persistence;
using NStore.Snapshots;
using NStore.Streams;
using Xunit;

// ReSharper disable InconsistentNaming

namespace NStore.Tests.AggregatesTests
{
    public abstract class BaseRepositoryTest
    {
        protected IStreamsFactory Streams { get; }
        protected IPersistence Persistence { get; }
        private IAggregateFactory AggregateFactory { get; }
        protected ISnapshotStore Snapshots { get; set; }
        private IRepository _repository;
        protected IRepository Repository => _repository ?? (_repository = CreateRepository());

        protected BaseRepositoryTest()
        {
            Persistence = new InMemoryPersistence();

            Streams = new StreamsFactory(Persistence);
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
        public async Task loading_an_aggregate_from_an_empty_stream_should_return_a_new_aggregate()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");

            Assert.NotNull(ticket);
            Assert.True(ticket.IsNew());
        }

        [Fact]
        public async Task saving_an_aggregate_should_persist_stream()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");

            ticket.Sale();

            await Repository.Save(ticket, "op_1");

            // load stream
            var stream = Streams.Open("Ticket_1");
            var tape = new Recorder();
            await stream.ReadAsync(tape, 0);

            Assert.Equal(1, tape.Length);
            Assert.IsType<Changeset>(tape[0].Payload);
        }

        [Fact]
        public async Task save_can_add_custom_info_in_headers()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            ticket.Sale();
            await Repository.Save(ticket, "op_1", h => h.Add("a", "b"));

            // load stream
            var stream = Streams.Open("Ticket_1");
            var tape = new Recorder();
            await stream.ReadAsync(tape, 0);

            var changeSet = (Changeset)tape[0].Payload;
            Assert.True(changeSet.Headers.ContainsKey("a"));
            Assert.Equal("b", changeSet.Headers["a"]);
        }

        [Fact]
        public async Task saving_twice_an_aggregate_should_persist_events_only_once()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            ticket.Sale();
            await Repository.Save(ticket, "op_1");
            await Repository.Save(ticket, "op_2");

            // load stream
            var stream = Streams.Open("Ticket_1");
            var tape = new Recorder();
            await stream.ReadAsync(tape, 0);
            Assert.Equal(1, tape.Length);
        }
    }

    public class with_populated_stream : BaseRepositoryTest
    {
        public with_populated_stream()
        {
            Persistence.AppendAsync("Ticket_1", 1, new Changeset(1, new TicketSold())).Wait();
            Persistence.AppendAsync("Ticket_1", 2, new Changeset(2, new TicketRefunded())).Wait();
        }

        [Fact]
        public async Task can_load_ticket_at_latest_version()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            Assert.True(ticket.IsInitialized);
            Assert.Equal(2, ticket.Version);
        }

        [Fact]
        public async Task cannot_save_aggregate_loaded_by_another_repository()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            ticket.Refund();

            var repo2 = CreateRepository();

            var ex = await Assert.ThrowsAsync<RepositoryMismatchException>(() =>
                repo2.Save(ticket, Guid.NewGuid().ToString())
            );
        }

        [Fact]
        public async Task loading_aggregate_twice_from_repository_should_return_same_istance()
        {
            var ticket1 = await Repository.GetById<Ticket>("Ticket_1");
            var ticket2 = await Repository.GetById<Ticket>("Ticket_1");

            Assert.Same(ticket1, ticket2);
        }
    }

    public class with_snapshots : BaseRepositoryTest
    {
        public with_snapshots()
        {
            Snapshots = new DefaultSnapshotStore(new InMemoryPersistence());

            Persistence.AppendAsync("Ticket_1", 1, new Changeset(1, new TicketSold())).Wait();
            Persistence.AppendAsync("Ticket_1", 2, new Changeset(2, new TicketRefunded())).Wait();
        }

        [Fact]
        public async Task can_load_without_snapshot()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            Assert.Equal(2, ticket.Version);
        }

        [Fact]
        public async Task saving_should_create_snapshot()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            ticket.Refund();
            await Repository.Save(ticket, "save_snap");

            var snapshot = await Snapshots.GetAsync("Ticket_1", int.MaxValue);
            Assert.NotNull(snapshot);
            Assert.Equal(3, snapshot.SourceVersion);
            Assert.NotNull(snapshot.Payload);
            Assert.Equal("1", snapshot.SchemaVersion);
        }

        [Fact]
        public async Task saving_new_aggregate_should_create_snapshot()
        {
            var ticket = await Repository.GetById<Ticket>("new_ticket");
            ticket.Sale();
            await Repository.Save(ticket, "save_snap");

            var snapshot = await Snapshots.GetAsync("new_ticket", int.MaxValue);
            Assert.NotNull(snapshot);
            Assert.Equal(1, snapshot.SourceVersion);
            Assert.NotNull(snapshot.Payload);
            Assert.Equal("1", snapshot.SchemaVersion);
        }
    }

    public class with_snapshot_only : BaseRepositoryTest
    {
        public with_snapshot_only()
        {
            Snapshots = new DefaultSnapshotStore(new InMemoryPersistence());
        }

        [Fact]
        public async Task with_snapshot_but_without_stream_should_throw_stale_aggregate_exception()
        {
            var ticketState = new TicketState();
            var snapshot = new SnapshotInfo("Ticket_1", 2, ticketState, "1");
            await Snapshots.AddAsync("Ticket_1", snapshot);

            var ex = await Assert.ThrowsAsync<StaleSnapshotException>(() =>
                Repository.GetById<Ticket>("Ticket_1")
            );

            Assert.Equal("Ticket_1", ex.AggregateId);
            Assert.Equal(2, ex.AggregateVersion);
        }
    }

    public class repository_should_not_persist_empty_changeset : BaseRepositoryTest
    {
        [Fact]
        public async Task with_snapshot_but_without_stream_should_throw_stale_aggregate_exception()
        {
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            await Repository.Save(ticket, "empty");

            var chunk = await Persistence.ReadSingleBackwardAsync("Ticket_1");

            Assert.Null(chunk);
        }
    }

    public class repository_should_persist_empty_changeset : BaseRepositoryTest
    {
        [Fact]
        public async Task with_snapshot_but_without_stream_should_throw_stale_aggregate_exception()
        {
            ((Repository) Repository).PersistEmptyChangeset = true;
            var ticket = await Repository.GetById<Ticket>("Ticket_1");
            await Repository.Save(ticket, "empty");

            var chunk = await Persistence.ReadSingleBackwardAsync("Ticket_1");

            Assert.NotNull(chunk);
            Assert.IsType<Changeset>(chunk.Payload);
            Assert.True(((Changeset) chunk.Payload).IsEmpty);
            Assert.Equal("empty", chunk.OperationId);
        }
    }

}