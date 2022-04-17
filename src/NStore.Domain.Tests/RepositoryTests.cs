// ReSharper disable InconsistentNaming

using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using NStore.Core.Streams;
using System;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable S101 // Types should be named in camel case

namespace NStore.Domain.Tests
{
    public abstract class BaseRepositoryTest
    {
        protected IStreamsFactory _streams;
        protected IStreamsFactory Streams => _streams ??= new StreamsFactory(Store);
        protected IPersistence _store;
        protected IPersistence Store => _store ??= new InMemoryStore(new InMemoryPersistenceOptions());
        private IAggregateFactory AggregateFactory { get; }
        protected ISnapshotStore Snapshots { get; set; }
        private IRepository _repository;
        protected IRepository Repository => _repository ??= CreateRepository();

        protected BaseRepositoryTest()
        {
            AggregateFactory = new DefaultAggregateFactory();
        }

        protected virtual IRepository CreateRepository()
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
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);

            Assert.NotNull(ticket);
            Assert.True(ticket.IsNew());
        }

        [Fact]
        public async Task saving_an_aggregate_should_persist_stream()
        {
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);

            ticket.Sale();

            await Repository.SaveAsync(ticket, "op_1").ConfigureAwait(false);

            // load stream
            var stream = Streams.Open("Ticket_1");
            var tape = new Recorder();
            await stream.ReadAsync(tape, 0).ConfigureAwait(false);

            Assert.Equal(1, tape.Length);
            Assert.IsType<Changeset>(tape[0].Payload);
        }

        [Fact]
        public async Task save_can_add_custom_info_in_headers()
        {
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            ticket.Sale();
            await Repository.SaveAsync(ticket, "op_1", h => h.Add("a", "b")).ConfigureAwait(false);

            // load stream
            var stream = Streams.Open("Ticket_1");
            var tape = new Recorder();
            await stream.ReadAsync(tape, 0).ConfigureAwait(false);

            var changeSet = (Changeset)tape[0].Payload;
            Assert.True(changeSet.Headers.ContainsKey("a"));
            Assert.Equal("b", changeSet.Headers["a"]);
        }

        [Fact]
        public async Task saving_twice_an_aggregate_should_persist_events_only_once()
        {
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            ticket.Sale();
            await Repository.SaveAsync(ticket, "op_1").ConfigureAwait(false);
            await Repository.SaveAsync(ticket, "op_2").ConfigureAwait(false);

            // load stream
            var stream = Streams.Open("Ticket_1");
            var tape = new Recorder();
            await stream.ReadAsync(tape, 0).ConfigureAwait(false);
            Assert.Equal(1, tape.Length);
        }
    }

    public class with_populated_stream : BaseRepositoryTest
    {
        public with_populated_stream()
        {
            Store.AppendAsync("Ticket_1", 1, new Changeset(1, new object[] { new TicketSold() })).Wait();
            Store.AppendAsync("Ticket_1", 2, new Changeset(2, new object[] { new TicketRefunded() })).Wait();
        }

        [Fact]
        public async Task can_load_ticket_at_latest_version()
        {
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            Assert.True(ticket.IsInitialized);
            Assert.Equal(2, ticket.Version);
        }

        [Fact]
        public async Task cannot_save_aggregate_loaded_by_another_repository()
        {
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            ticket.Refund();

            var repo2 = CreateRepository();

            var ex = await Assert.ThrowsAsync<RepositoryMismatchException>(() =>
                repo2.SaveAsync(ticket, Guid.NewGuid().ToString())
            ).ConfigureAwait(false);
        }

        [Fact]
        public async Task loading_aggregate_twice_from_repository_should_return_same_istance()
        {
            var ticket1 = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            var ticket2 = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);

            Assert.Same(ticket1, ticket2);
        }
    }

    public class with_snapshots : BaseRepositoryTest
    {
        public with_snapshots()
        {
            Snapshots = new DefaultSnapshotStore(new InMemoryStore(new InMemoryPersistenceOptions()));

            Store.AppendAsync("Ticket_1", 1, new Changeset(1, new object[] { new TicketSold() })).Wait();
            Store.AppendAsync("Ticket_1", 2, new Changeset(2, new object[] { new TicketRefunded() })).Wait();
        }

        [Fact]
        public async Task can_load_without_snapshot()
        {
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            Assert.Equal(2, ticket.Version);
        }

        [Fact]
        public async Task saving_should_create_snapshot()
        {
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            ticket.Refund();
            await Repository.SaveAsync(ticket, "save_snap").ConfigureAwait(false);

            var snapshot = await Snapshots.GetAsync("Ticket_1", int.MaxValue).ConfigureAwait(false);
            Assert.NotNull(snapshot);
            Assert.Equal(3, snapshot.SourceVersion);
            Assert.NotNull(snapshot.Payload);
            Assert.Equal("1", snapshot.SchemaVersion);
        }

        [Fact]
        public async Task saving_new_aggregate_should_create_snapshot()
        {
            var ticket = await Repository.GetByIdAsync<Ticket>("new_ticket").ConfigureAwait(false);
            ticket.Sale();
            await Repository.SaveAsync(ticket, "save_snap").ConfigureAwait(false);

            var snapshot = await Snapshots.GetAsync("new_ticket", int.MaxValue).ConfigureAwait(false);
            Assert.NotNull(snapshot);
            Assert.Equal(1, snapshot.SourceVersion);
            Assert.NotNull(snapshot.Payload);
            Assert.Equal("1", snapshot.SchemaVersion);
        }
    }

    public class with_snapshots_and_idempotent_command : BaseRepositoryTest
    {
        public with_snapshots_and_idempotent_command()
        {
            Snapshots = new DefaultSnapshotStore(new InMemoryStore(new InMemoryPersistenceOptions()));
        }

        [Fact]
        public async Task idempotent_save_should_not_snapshot()
        {
            //Arrange, save with one commit.
            String operationId = Guid.NewGuid().ToString();
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            ticket.Sale();
            await Repository.SaveAsync(ticket, "save_id").ConfigureAwait(false);

            //now generate an event dosomething.
            ticket.DoSomething();
            await Repository.SaveAsync(ticket, operationId).ConfigureAwait(false);

            //new repository reload the aggregate, and try to save with the very same operation id, this should not generate commit.
            var newRepository = CreateRepository();
            ticket = await newRepository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            ticket.DoSomething();
            await newRepository.SaveAsync(ticket, operationId).ConfigureAwait(false); //idempotent

            //Verify that the aggregate is in version 2
            var chunk = await Store.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);
            Assert.NotNull(chunk);
            Assert.IsType<Changeset>(chunk.Payload);
            Assert.Equal(2, ((Changeset)chunk.Payload).AggregateVersion);

            //Verify snapshot is in version 2.
            var snapshot = await Snapshots.GetAsync("Ticket_1", int.MaxValue).ConfigureAwait(false);
            Assert.NotNull(snapshot);
            Assert.Equal(2, snapshot.SourceVersion);

            //now we should be able to reload the aggregate with snapshot.
            var finalRepository = CreateRepository();
            ticket = await finalRepository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            Assert.Equal(2, ticket.Version);
        }
    }

    public class with_snapshot_only : BaseRepositoryTest
    {
        public with_snapshot_only()
        {
            Snapshots = new DefaultSnapshotStore(new InMemoryStore(new InMemoryPersistenceOptions()));
        }

        [Fact]
        public async Task with_snapshot_but_without_stream_should_throw_stale_aggregate_exception()
        {
            var ticketState = new TicketState();
            var snapshot = new SnapshotInfo("Ticket_1", 2, ticketState, "1");
            await Snapshots.AddAsync("Ticket_1", snapshot).ConfigureAwait(false);

            var ex = await Assert.ThrowsAsync<StaleSnapshotException>(() =>
                Repository.GetByIdAsync<Ticket>("Ticket_1")
            ).ConfigureAwait(false);

            Assert.Equal("Ticket_1", ex.AggregateId);
            Assert.Equal(2, ex.AggregateVersion);
        }
    }

    public class repository_should_not_persist_empty_changeset : BaseRepositoryTest
    {
        [Fact]
        public async Task with_snapshot_but_without_stream_should_throw_stale_aggregate_exception()
        {
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            await Repository.SaveAsync(ticket, "empty").ConfigureAwait(false);

            var chunk = await Store.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);

            Assert.Null(chunk);
        }
    }

    public class repository_should_persist_empty_changeset : BaseRepositoryTest
    {
        [Fact]
        public async Task with_snapshot_but_without_stream_should_throw_stale_aggregate_exception()
        {
            ((Repository)Repository).PersistEmptyChangeset = true;
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            await Repository.SaveAsync(ticket, "empty").ConfigureAwait(false);

            var chunk = await Store.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);

            Assert.NotNull(chunk);
            Assert.IsType<Changeset>(chunk.Payload);
            Assert.True(((Changeset)chunk.Payload).IsEmpty());
            Assert.Equal("empty", chunk.OperationId);
        }
    }

    public class handling_exception_on_streams : BaseRepositoryTest
    {
        [Fact]
        public async Task repository_should_throw_if_stream_throws_exception()
        {
            await Setup().ConfigureAwait(false);

            //After setup lets throw for all the reads
            networkSimulator.ShouldThrow = true;
            await Assert.ThrowsAsync<RepositoryReadException>(() =>
              Repository.GetByIdAsync<CounterAggregate>("Counter_1")).ConfigureAwait(false);
        }

        AlwaysThrowsNetworkSimulator networkSimulator;
        protected IPersistence CreatePersistence()
        {
            networkSimulator = new AlwaysThrowsNetworkSimulator();
            var options = new InMemoryPersistenceOptions
            {
                NetworkSimulator = networkSimulator
            };
            var persistence = new InMemoryStore(options);
            return persistence;
        }

        private async Task Setup()
        {
            _store = CreatePersistence();
            var counter = await Repository.GetByIdAsync<CounterAggregate>("Counter_1").ConfigureAwait(false);
            counter.Increment();
            await Repository.SaveAsync(counter, "inc_1").ConfigureAwait(false);

            Repository.Clear();
        }
    }

    public class invariants_on_repository : BaseRepositoryTest
    {
        [Fact]
        public async Task repository_should_not_throw_invariant_exception_on_duplicated_operations()
        {
            await Setup().ConfigureAwait(false);
            var counter = await Repository.GetByIdAsync<CounterAggregate>("Counter").ConfigureAwait(false);
            counter.Decrement();
            await Repository.SaveAsync(counter, "dec_1").ConfigureAwait(false);
        }

        [Fact]
        public async Task repository_should_throw_invariant_exception()
        {
            await Setup().ConfigureAwait(false);
            var counter = await Repository.GetByIdAsync<CounterAggregate>("Counter").ConfigureAwait(false);
            counter.Decrement();

            var ex = await Assert.ThrowsAsync<InvariantCheckFailedException>(() =>
                Repository.SaveAsync(counter, "dec_2")
            ).ConfigureAwait(false);
        }

        private async Task Setup()
        {
            var counter = await Repository.GetByIdAsync<CounterAggregate>("Counter").ConfigureAwait(false);
            counter.Increment();
            await Repository.SaveAsync(counter, "inc_1").ConfigureAwait(false);

            counter.Decrement();
            await Repository.SaveAsync(counter, "dec_1").ConfigureAwait(false);

            Repository.Clear();
        }
    }

    /// <summary>
    /// Verify that we can clear the repository when a concurrency exception is thrown
    /// and we can re-use the same instance to reload the aggregate e retry the operation.
    /// </summary>
    public class identity_map_management : BaseRepositoryTest
    {
        [Fact]
        public async Task when_concurrency_exception_we_can_clear_the_identity_map()
        {
            //Arrange save a ticket in version 1.
            var ticket = await Repository.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            ticket.Sale();
            await Repository.SaveAsync(ticket, "sale").ConfigureAwait(false);

            var repository1 = CreateRepository();
            var repository2 = CreateRepository();

            //now load aggregate
            var ticket1 = await repository1.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            var ticket2 = await repository2.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);

            ticket1.DoSomething();
            await repository1.SaveAsync(ticket1, Guid.NewGuid().ToString()).ConfigureAwait(false);

            //Act, now we expect the second repository to have an exception
            await Assert.ThrowsAsync<ConcurrencyException>(async () =>
            {
                ticket2.DoSomething();
                await repository2.SaveAsync(ticket2, Guid.NewGuid().ToString()).ConfigureAwait(false);
            }).ConfigureAwait(false);

            //ok now we should be able to clear, reload and save
            repository2.Clear();
            ticket2 = await repository2.GetByIdAsync<Ticket>("Ticket_1").ConfigureAwait(false);
            ticket2.DoSomething();
            await repository2.SaveAsync(ticket2, Guid.NewGuid().ToString()).ConfigureAwait(false);

            var chunk = await Store.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);

            Assert.NotNull(chunk);
            Assert.IsType<Changeset>(chunk.Payload);
            Assert.Equal(3, ((Changeset)chunk.Payload).AggregateVersion);
        }
    }

    public class with_repository : BaseRepositoryTest
    {
        [Fact]
        public async Task should_save_aggregate_created_by_external_factory()
        {
            // arrange
            var ticket = Ticket.CreateNew("Ticket_1");
            ticket.Sale();
            
            // act
            await Repository.SaveAsync(ticket, "new");

            // assert
            var chunk = await Store.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);

            Assert.NotNull(chunk);
            Assert.IsType<Changeset>(chunk.Payload);
            Assert.Equal(1, ((Changeset)chunk.Payload).AggregateVersion);
        }

        [Fact]
        public async Task should_save_twice_aggregate_created_by_external_factory()
        {
            // arrange
            var ticket = Ticket.CreateNew("Ticket_1");
            ticket.Sale();
            await Repository.SaveAsync(ticket, "new");
            
            // act
            ticket.Refund();
            await Repository.SaveAsync(ticket, "update");
            
            // assert
            var chunk = await Store.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);

            Assert.NotNull(chunk);
            Assert.IsType<Changeset>(chunk.Payload);
            Assert.Equal(2, ((Changeset)chunk.Payload).AggregateVersion);
            
        }
    }
}
#pragma warning restore S101 // Types should be named in camel case
