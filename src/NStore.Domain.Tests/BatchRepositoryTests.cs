using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace NStore.Domain.Tests
{
    public abstract class BaseBatchRepositoryTest
    {
        protected IPersistence _persistence;
        protected IPersistence Persistence => _persistence ?? (_persistence = CreatePersistence());
        protected IAggregateFactory AggregateFactory { get; set; }
        protected ISnapshotBatchStore SnapshotBatchStore { get; set; }
        private IBatchRepository _batchRepository;
        protected IBatchRepository BatchRepository => _batchRepository ?? (_batchRepository = CreateBatchRepository());

        protected BaseBatchRepositoryTest()
        {
            AggregateFactory = new DefaultAggregateFactory();
        }

        protected virtual IPersistence CreatePersistence()
        {
            return new InMemoryPersistence(new InMemoryPersistenceOptions());
        }

        protected virtual IBatchRepository CreateBatchRepository()
        {
            if (!(Persistence is IMultiPartitionPersistenceReader multiReader))
            {
                throw new InvalidOperationException("Persistence must implement IMultiPartitionPersistenceReader");
            }

            if (!(Persistence is IEnhancedPersistence enhancedPersistence))
            {
                throw new InvalidOperationException("Persistence must implement IEnhancedPersistence");
            }

            return new BatchRepository(
                AggregateFactory,
                multiReader,
                enhancedPersistence,
                SnapshotBatchStore
            );
        }
    }

    public class batch_with_empty_store : BaseBatchRepositoryTest
    {
        [Fact]
        public async Task loading_empty_batch_should_return_empty_dictionary()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new string[0]).ConfigureAwait(false);

            Assert.NotNull(tickets);
            Assert.Empty(tickets);
        }

        [Fact]
        public async Task loading_single_aggregate_should_return_new_aggregate()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);

            Assert.Single(tickets);
            Assert.True(tickets["Ticket_1"].IsNew());
        }

        [Fact]
        public async Task loading_multiple_aggregates_should_return_all_as_new()
        {
            var ids = new[] { "Ticket_1", "Ticket_2", "Ticket_3" };
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(ids).ConfigureAwait(false);

            Assert.Equal(3, tickets.Count);
            Assert.All(tickets.Values, t => Assert.True(t.IsNew()));
        }

        [Fact]
        public async Task saving_empty_batch_should_not_throw()
        {
            var result = await BatchRepository.SaveManyAsync(new IAggregate[0], "op_1").ConfigureAwait(false);
            Assert.NotNull(result);
            // Should complete without error
        }

        [Fact]
        public async Task saving_single_aggregate_should_persist_stream()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();

            var result = await BatchRepository.SaveManyAsync(tickets.Values, "op_1").ConfigureAwait(false);
            Assert.NotNull(result);

            var chunk = await Persistence.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);
            Assert.NotNull(chunk);
            Assert.IsType<Changeset>(chunk.Payload);
        }

        [Fact]
        public async Task saving_multiple_aggregates_should_persist_all_streams()
        {
            var ids = new[] { "Ticket_1", "Ticket_2", "Ticket_3" };
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(ids).ConfigureAwait(false);

            foreach (var ticket in tickets.Values)
            {
                ticket.Sale();
            }

            var result = await BatchRepository.SaveManyAsync(tickets.Values, "op_1").ConfigureAwait(false);
            Assert.NotNull(result);

            foreach (var id in ids)
            {
                var chunk = await Persistence.ReadSingleBackwardAsync(id).ConfigureAwait(false);
                Assert.NotNull(chunk);
                Assert.IsType<Changeset>(chunk.Payload);
            }
        }

        [Fact]
        public async Task saving_aggregates_with_no_changes_should_not_persist()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2" }).ConfigureAwait(false);

            var result = await BatchRepository.SaveManyAsync(tickets.Values, "op_1").ConfigureAwait(false);
            Assert.NotNull(result);

            var chunk1 = await Persistence.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);
            var chunk2 = await Persistence.ReadSingleBackwardAsync("Ticket_2").ConfigureAwait(false);
            Assert.Null(chunk1);
            Assert.Null(chunk2);
        }

        [Fact]
        public async Task saving_mixed_changed_and_unchanged_aggregates()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2", "Ticket_3" }).ConfigureAwait(false);

            tickets["Ticket_1"].Sale();
            // Ticket_2 unchanged
            tickets["Ticket_3"].Sale();

            var result = await BatchRepository.SaveManyAsync(tickets.Values, "op_1").ConfigureAwait(false);
            Assert.NotNull(result);

            Assert.NotNull(await Persistence.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false));
            Assert.Null(await Persistence.ReadSingleBackwardAsync("Ticket_2").ConfigureAwait(false));
            Assert.NotNull(await Persistence.ReadSingleBackwardAsync("Ticket_3").ConfigureAwait(false));
        }

        [Fact]
        public async Task loading_same_aggregates_twice_should_return_cached_instances()
        {
            var tickets1 = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            var tickets2 = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);

            Assert.Same(tickets1["Ticket_1"], tickets2["Ticket_1"]);
        }
    }

    public class batch_with_populated_streams : BaseBatchRepositoryTest
    {
        public batch_with_populated_streams()
        {
            Persistence.AppendAsync("Ticket_1", 1, new Changeset(1, new object[] { new TicketSold() })).Wait();
            Persistence.AppendAsync("Ticket_1", 2, new Changeset(2, new object[] { new TicketRefunded() })).Wait();

            Persistence.AppendAsync("Ticket_2", 1, new Changeset(1, new object[] { new TicketSold() })).Wait();

            Persistence.AppendAsync("Ticket_3", 1, new Changeset(1, new object[] { new TicketSold() })).Wait();
            Persistence.AppendAsync("Ticket_3", 2, new Changeset(2, new object[] { new TicketRefunded() })).Wait();
            Persistence.AppendAsync("Ticket_3", 3, new Changeset(3, new object[] { new TicketSold() })).Wait();
        }

        [Fact]
        public async Task can_load_multiple_tickets_at_latest_versions()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(
                new[] { "Ticket_1", "Ticket_2", "Ticket_3" }
            ).ConfigureAwait(false);

            Assert.Equal(3, tickets.Count);
            Assert.Equal(2, tickets["Ticket_1"].Version);
            Assert.Equal(1, tickets["Ticket_2"].Version);
            Assert.Equal(3, tickets["Ticket_3"].Version);
        }

        [Fact]
        public async Task can_load_subset_of_tickets()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(
                new[] { "Ticket_1", "Ticket_3" }
            ).ConfigureAwait(false);

            Assert.Equal(2, tickets.Count);
            Assert.Equal(2, tickets["Ticket_1"].Version);
            Assert.Equal(3, tickets["Ticket_3"].Version);
        }

        [Fact]
        public async Task can_save_multiple_aggregates_after_load()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(
                new[] { "Ticket_1", "Ticket_2" }
            ).ConfigureAwait(false);

            tickets["Ticket_1"].DoSomething();
            tickets["Ticket_2"].Refund();

            var result = await BatchRepository.SaveManyAsync(tickets.Values, "op_1").ConfigureAwait(false);
            Assert.NotNull(result);

            var chunk1 = await Persistence.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);
            var chunk2 = await Persistence.ReadSingleBackwardAsync("Ticket_2").ConfigureAwait(false);

            Assert.Equal(3, ((Changeset)chunk1.Payload).AggregateVersion);
            Assert.Equal(2, ((Changeset)chunk2.Payload).AggregateVersion);
        }

        [Fact]
        public async Task cannot_save_aggregate_loaded_by_another_batch_repository()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets["Ticket_1"].DoSomething();

            var repo2 = CreateBatchRepository();

            var ex = await Assert.ThrowsAsync<RepositoryMismatchException>(() =>
                repo2.SaveManyAsync(tickets.Values, Guid.NewGuid().ToString())
            ).ConfigureAwait(false);
        }
    }

    public class batch_with_concurrency_conflicts : BaseBatchRepositoryTest
    {
        [Fact]
        public async Task concurrent_modification_of_single_aggregate_should_throw_batch_concurrency_exception()
        {
            // Setup initial state
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();
            var initResult = await BatchRepository.SaveManyAsync(tickets.Values, "initial").ConfigureAwait(false);
            Assert.NotNull(initResult);

            // Load same aggregate in two repositories
            var repo1 = CreateBatchRepository();
            var repo2 = CreateBatchRepository();

            var tickets1 = await repo1.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            var tickets2 = await repo2.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);

            // First repo saves successfully
            tickets1["Ticket_1"].DoSomething();
            var save1 = await repo1.SaveManyAsync(tickets1.Values, Guid.NewGuid().ToString()).ConfigureAwait(false);
            Assert.NotNull(save1);

            // Second repo should report concurrency conflict in result
            tickets2["Ticket_1"].DoSomething();
            var result = await repo2.SaveManyAsync(tickets2.Values, Guid.NewGuid().ToString()).ConfigureAwait(false);

            Assert.NotNull(result);
            Assert.False(result.Success);
            Assert.True(result.HasFailures);
            Assert.Single(result.Results);
            var failedResult = result.Results[0];
            Assert.Equal("Ticket_1", failedResult.AggregateId);
            Assert.False(failedResult.Succeeded);
            Assert.NotNull(failedResult.FailureException);
            Assert.Null(failedResult.Chunk);
        }

        [Fact]
        public async Task concurrent_modification_in_batch_should_report_all_conflicts()
        {
            // Setup initial state
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(
                new[] { "Ticket_1", "Ticket_2", "Ticket_3" }
            ).ConfigureAwait(false);

            foreach (var ticket in tickets.Values)
            {
                ticket.Sale();
            }
            var initAll = await BatchRepository.SaveManyAsync(tickets.Values, "initial").ConfigureAwait(false);
            Assert.NotNull(initAll);

            // Load in two repositories
            var repo1 = CreateBatchRepository();
            var repo2 = CreateBatchRepository();

            var tickets1 = await repo1.GetManyByIdAsync<Ticket>(
                new[] { "Ticket_1", "Ticket_2" }
            ).ConfigureAwait(false);

            var tickets2 = await repo2.GetManyByIdAsync<Ticket>(
                new[] { "Ticket_1", "Ticket_2", "Ticket_3" }
            ).ConfigureAwait(false);

            // Repo1 modifies Ticket_1 and Ticket_2
            tickets1["Ticket_1"].DoSomething();
            tickets1["Ticket_2"].DoSomething();
            var save2 = await repo1.SaveManyAsync(tickets1.Values, Guid.NewGuid().ToString()).ConfigureAwait(false);
            Assert.NotNull(save2);

            // Repo2 tries to modify all three - should report partial success
            tickets2["Ticket_1"].DoSomething();
            tickets2["Ticket_2"].DoSomething();
            tickets2["Ticket_3"].DoSomething();

            var result = await repo2.SaveManyAsync(tickets2.Values, Guid.NewGuid().ToString()).ConfigureAwait(false);

            Assert.NotNull(result);
            Assert.False(result.Success);
            Assert.True(result.HasFailures);
            Assert.Equal(3, result.Results.Count);

            var failed = result.Results.Where(r => !r.Succeeded).ToList();
            var succeeded = result.Results.Where(r => r.Succeeded).ToList();

            Assert.Equal(2, failed.Count); // Ticket_1 and Ticket_2 failed
            Assert.Contains(failed, f => f.AggregateId == "Ticket_1");
            Assert.Contains(failed, f => f.AggregateId == "Ticket_2");
            Assert.All(failed, f => Assert.NotNull(f.FailureException));
            Assert.All(failed, f => Assert.Null(f.Chunk));

            // Ticket_3 should have succeeded
            Assert.Single(succeeded);
            Assert.Contains(succeeded, s => s.AggregateId == "Ticket_3");
            Assert.All(succeeded, s => Assert.NotNull(s.Chunk));
        }

        [Fact]
        public async Task can_clear_and_retry_after_concurrency_exception()
        {
            // Setup initial state
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();
            var init = await BatchRepository.SaveManyAsync(tickets.Values, "initial").ConfigureAwait(false);
            Assert.NotNull(init);

            var repo1 = CreateBatchRepository();
            var repo2 = CreateBatchRepository();

            var tickets1 = await repo1.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            var tickets2 = await repo2.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);

            tickets1["Ticket_1"].DoSomething();
            var save2 = await repo1.SaveManyAsync(tickets1.Values, Guid.NewGuid().ToString()).ConfigureAwait(false);
            Assert.NotNull(save2);

            tickets2["Ticket_1"].DoSomething();
            var conflictResult = await repo2.SaveManyAsync(tickets2.Values, Guid.NewGuid().ToString()).ConfigureAwait(false);
            Assert.False(conflictResult.Success); // Verify conflict happened

            // Clear and retry
            repo2.Clear();
            var tickets2Retry = await repo2.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets2Retry["Ticket_1"].DoSomething();
            var retryResult = await repo2.SaveManyAsync(tickets2Retry.Values, Guid.NewGuid().ToString()).ConfigureAwait(false);
            Assert.NotNull(retryResult);

            var chunk = await Persistence.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);
            Assert.Equal(3, ((Changeset)chunk.Payload).AggregateVersion);
        }
    }

    public class batch_with_snapshots : BaseBatchRepositoryTest
    {
        private DefaultSnapshotStore _snapshotStore;

        public batch_with_snapshots()
        {
            _snapshotStore = new DefaultSnapshotStore(new InMemoryPersistence(new InMemoryPersistenceOptions()));
            SnapshotBatchStore = new DefaultSnapshotBatchStore(_snapshotStore);

            Persistence.AppendAsync("Ticket_1", 1, new Changeset(1, new object[] { new TicketSold() })).Wait();
            Persistence.AppendAsync("Ticket_1", 2, new Changeset(2, new object[] { new TicketRefunded() })).Wait();

            Persistence.AppendAsync("Ticket_2", 1, new Changeset(1, new object[] { new TicketSold() })).Wait();
        }

        [Fact]
        public async Task can_load_without_snapshots()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(
                new[] { "Ticket_1", "Ticket_2" }
            ).ConfigureAwait(false);

            Assert.Equal(2, tickets["Ticket_1"].Version);
            Assert.Equal(1, tickets["Ticket_2"].Version);
        }

        [Fact]
        public async Task saving_should_create_snapshots()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(
                new[] { "Ticket_1", "Ticket_2" }
            ).ConfigureAwait(false);

            tickets["Ticket_1"].DoSomething();
            tickets["Ticket_2"].Refund();

            var saveSnapResult = await BatchRepository.SaveManyAsync(tickets.Values, "save_snap").ConfigureAwait(false);
            Assert.NotNull(saveSnapResult);

            var snapshot1 = await _snapshotStore.GetAsync("Ticket_1", int.MaxValue).ConfigureAwait(false);
            var snapshot2 = await _snapshotStore.GetAsync("Ticket_2", int.MaxValue).ConfigureAwait(false);

            Assert.NotNull(snapshot1);
            Assert.Equal(3, snapshot1.SourceVersion);

            Assert.NotNull(snapshot2);
            Assert.Equal(2, snapshot2.SourceVersion);
        }

        [Fact]
        public async Task loading_with_snapshots_should_reduce_event_replay()
        {
            // Create snapshots at version 2
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(
                new[] { "Ticket_1", "Ticket_2" }
            ).ConfigureAwait(false);

            tickets["Ticket_1"].DoSomething();
            tickets["Ticket_2"].Refund();
            var saveSnapResult2 = await BatchRepository.SaveManyAsync(tickets.Values, "save_snap").ConfigureAwait(false);
            Assert.NotNull(saveSnapResult2);

            // Add more events
            Persistence.AppendAsync("Ticket_1", 4, new Changeset(4, new object[] { new TicketSomethingHappened() })).Wait();
            Persistence.AppendAsync("Ticket_2", 3, new Changeset(3, new object[] { new TicketSomethingHappened() })).Wait();

            // Load in new repository - should use snapshots
            var repo2 = CreateBatchRepository();
            var ticketsWithSnap = await repo2.GetManyByIdAsync<Ticket>(
                new[] { "Ticket_1", "Ticket_2" }
            ).ConfigureAwait(false);

            Assert.Equal(4, ticketsWithSnap["Ticket_1"].Version);
            Assert.Equal(3, ticketsWithSnap["Ticket_2"].Version);
        }
    }

    public class batch_with_mixed_aggregate_types : BaseBatchRepositoryTest
    {
        [Fact]
        public async Task can_save_different_aggregate_types()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            var counters = await BatchRepository.GetManyByIdAsync<CounterAggregate>(new[] { "Counter_1" }).ConfigureAwait(false);

            tickets["Ticket_1"].Sale();
            counters["Counter_1"].Increment();

            var result = await BatchRepository.SaveManyAsync(new IAggregate[] { tickets["Ticket_1"], counters["Counter_1"] }, "op_1").ConfigureAwait(false);
            Assert.NotNull(result);

            var chunk1 = await Persistence.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);
            var chunk2 = await Persistence.ReadSingleBackwardAsync("Counter_1").ConfigureAwait(false);

            Assert.NotNull(chunk1);
            Assert.NotNull(chunk2);
        }
    }

    public class batch_repository_should_persist_empty_changeset : BaseBatchRepositoryTest
    {
        [Fact]
        public async Task when_persist_empty_changeset_is_true()
        {
            ((BatchRepository)BatchRepository).PersistEmptyChangeset = true;

            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            var result = await BatchRepository.SaveManyAsync(tickets.Values, "empty").ConfigureAwait(false);
            Assert.NotNull(result);

            var chunk = await Persistence.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);

            Assert.NotNull(chunk);
            Assert.IsType<Changeset>(chunk.Payload);
            Assert.True(((Changeset)chunk.Payload).IsEmpty());
        }
    }

    public class batch_idempotency_tests : BaseBatchRepositoryTest
    {
        [Fact]
        public async Task saving_with_same_operation_id_should_be_idempotent()
        {
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();

            // Save first time
            var first = await BatchRepository.SaveManyAsync(tickets.Values, "op_123").ConfigureAwait(false);
            Assert.NotNull(first);

            // Create new repository and load
            var repo2 = CreateBatchRepository();
            var tickets2 = await repo2.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets2["Ticket_1"].DoSomething();

            // Save with same operation ID - should not throw
            var second = await repo2.SaveManyAsync(tickets2.Values, "op_123").ConfigureAwait(false);
            Assert.NotNull(second);

            // Verify only one changeset was persisted
            var chunks = new List<IChunk>();
            await Persistence.ReadForwardAsync("Ticket_1", 0, new LambdaSubscription(c =>
            {
                chunks.Add(c);
                return Task.FromResult(true);
            }), long.MaxValue, int.MaxValue, default).ConfigureAwait(false);

            Assert.Single(chunks);
        }
    }
}
