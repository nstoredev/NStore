using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
        public async Task SaveManyAsync_should_report_invariant_failure_when_check_fails()
        {
            // Arrange - counter aggregate will return invalid invariant when decremented below zero
            var counters = await BatchRepository.GetManyByIdAsync<CounterAggregate>(new[] { "Counter_1" }).ConfigureAwait(false);
            counters["Counter_1"].Decrement(); // now invalid (value becomes -1)

            // Act
            var result = await BatchRepository.SaveManyAsync(counters.Values, "inv_op").ConfigureAwait(false);

            // Assert
            Assert.NotNull(result);
            Assert.False(result.Success);
            Assert.True(result.HasFailures);
            Assert.Single(result.Results);
            var res = result.Results[0];
            Assert.Equal("Counter_1", res.AggregateId);
            Assert.False(res.Succeeded);
            Assert.Equal(AggregateSaveFailureKind.InvariantFailure, res.FailureKind);
            Assert.Null(res.Chunk);
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
        public async Task SaveManyAsync_should_throw_when_duplicate_aggregates_passed()
        {
            // Arrange - load a single aggregate and duplicate it in the input list
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            var list = tickets.Values.ToList();
            list.Add(tickets["Ticket_1"]); // duplicate id in input

            // Act / Assert
            await Assert.ThrowsAsync<ArgumentException>(() => BatchRepository.SaveManyAsync(list, "dup_op")).ConfigureAwait(false);
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

            await Assert.ThrowsAsync<RepositoryMismatchException>(() =>
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
            Assert.Equal(AggregateSaveFailureKind.Concurrency, failedResult.FailureKind);
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
            Assert.All(failed, f => Assert.Equal(AggregateSaveFailureKind.Concurrency, f.FailureKind));
            Assert.All(failed, f => Assert.Null(f.Chunk));

            // Ticket_3 should have succeeded
            Assert.Single(succeeded);
            Assert.Contains(succeeded, s => s.AggregateId == "Ticket_3");
            Assert.All(succeeded, s => Assert.NotNull(s.Chunk));
            Assert.All(succeeded, s => Assert.Null(s.FailureKind));
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
        private readonly DefaultSnapshotStore _snapshotStore;

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
            await Persistence.AppendAsync("Ticket_1", 4, new Changeset(4, new object[] { new TicketSomethingHappened() }));
            await Persistence.AppendAsync("Ticket_2", 3, new Changeset(3, new object[] { new TicketSomethingHappened() }));

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
            Assert.Single(second.Results);
            var opResult = second.Results[0];
            Assert.True(opResult.Succeeded);
            Assert.Equal(AggregateSaveFailureKind.DuplicatedOperation, opResult.FailureKind);

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

    public class batch_with_stale_snapshots : BaseBatchRepositoryTest
    {
        private readonly DefaultSnapshotStore _snapshotStore;

        public batch_with_stale_snapshots()
        {
            _snapshotStore = new DefaultSnapshotStore(new InMemoryPersistence(new InMemoryPersistenceOptions()));
            SnapshotBatchStore = new DefaultSnapshotBatchStore(_snapshotStore);
        }

        [Fact]
        public async Task loading_aggregate_with_stale_snapshot_should_throw_StaleSnapshotException()
        {
            // Arrange: Create a snapshot for an aggregate that has no events in the stream
            // This simulates a scenario where the snapshot exists but events were deleted/corrupted
            var snapshotInfo = new SnapshotInfo(
                "Ticket_Stale",
                5, // snapshot claims version 5
                new TicketState(),
                "1"
            );
            await _snapshotStore.AddAsync("Ticket_Stale", snapshotInfo).ConfigureAwait(false);

            // Act & Assert: Loading should throw StaleSnapshotException
            // because snapshot version > 0 but no events are found
            await Assert.ThrowsAsync<StaleSnapshotException>(
                () => BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_Stale" })
            ).ConfigureAwait(false);
        }

        [Fact]
        public async Task StaleSnapshotException_should_contain_aggregate_info()
        {
            // Arrange
            var snapshotInfo = new SnapshotInfo(
                "Ticket_Stale_Info",
                10,
                new TicketState(),
                "1"
            );
            await _snapshotStore.AddAsync("Ticket_Stale_Info", snapshotInfo).ConfigureAwait(false);

            // Act & Assert
            var ex = await Assert.ThrowsAsync<StaleSnapshotException>(
                () => BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_Stale_Info" })
            ).ConfigureAwait(false);

            Assert.Equal("Ticket_Stale_Info", ex.AggregateId);
            Assert.Equal(10, ex.AggregateVersion);
        }
    }

    public class batch_with_headers : BaseBatchRepositoryTest
    {
        [Fact]
        public async Task SaveManyAsync_should_invoke_headers_callback()
        {
            // Arrange
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();

            // Act
            var result = await BatchRepository.SaveManyAsync(
                tickets.Values,
                "op_headers",
                headers => headers.Add("CustomHeader", "CustomValue")
            ).ConfigureAwait(false);

            Assert.True(result.Success);

            // Assert - verify headers were added by reading the persisted chunk
            var chunk = await Persistence.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);
            Assert.NotNull(chunk);
            var changeset = (Changeset)chunk.Payload;
            Assert.True(changeset.Headers.ContainsKey("CustomHeader"));
            Assert.Equal("CustomValue", changeset.Headers["CustomHeader"]);
        }

        [Fact]
        public async Task SaveManyAsync_should_apply_headers_to_all_aggregates()
        {
            // Arrange
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();
            tickets["Ticket_2"].Sale();

            // Act
            await BatchRepository.SaveManyAsync(
                tickets.Values,
                "op_headers_multi",
                headers => headers.Add("BatchId", "12345")
            ).ConfigureAwait(false);

            // Assert
            var chunk1 = await Persistence.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);
            var chunk2 = await Persistence.ReadSingleBackwardAsync("Ticket_2").ConfigureAwait(false);

            Assert.Equal("12345", ((Changeset)chunk1.Payload).Headers["BatchId"]);
            Assert.Equal("12345", ((Changeset)chunk2.Payload).Headers["BatchId"]);
        }
    }

    public class batch_with_null_operation_id : BaseBatchRepositoryTest
    {
        [Fact]
        public async Task SaveManyAsync_with_null_operationId_should_generate_unique_id()
        {
            // Arrange
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();

            // Act - pass null as operationId
            var result = await BatchRepository.SaveManyAsync(tickets.Values, null).ConfigureAwait(false);

            // Assert - should succeed without throwing
            Assert.NotNull(result);
            Assert.True(result.Success);
            Assert.Single(result.Results);
            Assert.True(result.Results[0].Succeeded);

            // Verify data was persisted
            var chunk = await Persistence.ReadSingleBackwardAsync("Ticket_1").ConfigureAwait(false);
            Assert.NotNull(chunk);
        }

        [Fact]
        public async Task SaveManyAsync_with_null_operationId_should_not_be_idempotent()
        {
            // Arrange
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();

            // Act - save with null operationId twice
            var first = await BatchRepository.SaveManyAsync(tickets.Values, null).ConfigureAwait(false);
            Assert.True(first.Success);

            // Load again and save with null - should create a new event (not idempotent)
            var repo2 = CreateBatchRepository();
            var tickets2 = await repo2.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets2["Ticket_1"].DoSomething();

            var second = await repo2.SaveManyAsync(tickets2.Values, null).ConfigureAwait(false);

            // Assert - both saves should succeed (different generated operation IDs)
            Assert.True(second.Success);

            // Verify two chunks were persisted
            var chunks = new List<IChunk>();
            await Persistence.ReadForwardAsync("Ticket_1", 0, new LambdaSubscription(c =>
            {
                chunks.Add(c);
                return Task.FromResult(true);
            }), long.MaxValue, int.MaxValue, default).ConfigureAwait(false);

            Assert.Equal(2, chunks.Count);
        }
    }

    public class batch_save_result_properties : BaseBatchRepositoryTest
    {
        [Fact]
        public async Task BatchSaveResult_Empty_should_return_valid_empty_result()
        {
            // Act
            var result = await BatchRepository.SaveManyAsync(new IAggregate[0], "empty_op").ConfigureAwait(false);

            // Assert
            Assert.NotNull(result);
            Assert.Empty(result.Results);
            Assert.True(result.Success);
            Assert.False(result.HasFailures);
        }

        [Fact]
        public async Task BatchSaveResult_Success_should_be_true_when_all_aggregates_saved()
        {
            // Arrange
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();
            tickets["Ticket_2"].Sale();

            // Act
            var result = await BatchRepository.SaveManyAsync(tickets.Values, "success_op").ConfigureAwait(false);

            // Assert
            Assert.True(result.Success);
            Assert.False(result.HasFailures);
            Assert.Equal(2, result.Results.Count);
            Assert.All(result.Results, r => Assert.True(r.Succeeded));
            Assert.All(result.Results, r => Assert.NotNull(r.Chunk));
            Assert.All(result.Results, r => Assert.Null(r.FailureKind));
        }

        [Fact]
        public async Task BatchSaveResult_should_report_unchanged_aggregates()
        {
            // Arrange
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();
            // Ticket_2 unchanged

            // Act
            var result = await BatchRepository.SaveManyAsync(tickets.Values, "mixed_op").ConfigureAwait(false);

            // Assert
            Assert.True(result.Success); // Unchanged is still considered success
            Assert.Equal(2, result.Results.Count);

            var changedResult = result.Results.First(r => r.AggregateId == "Ticket_1");
            var unchangedResult = result.Results.First(r => r.AggregateId == "Ticket_2");

            Assert.True(changedResult.Succeeded);
            Assert.NotNull(changedResult.Chunk);
            Assert.Null(changedResult.FailureKind);

            Assert.True(unchangedResult.Succeeded);
            Assert.Null(unchangedResult.Chunk);
            Assert.Equal(AggregateSaveFailureKind.Unchanged, unchangedResult.FailureKind);
        }
    }

    public class batch_saving_new_aggregates_directly : BaseBatchRepositoryTest
    {
        [Fact]
        public async Task can_save_new_aggregate_without_loading_first()
        {
            // Arrange - create aggregate directly without loading via GetManyByIdAsync
            var ticket = Ticket.CreateNew("Direct_Ticket_1");
            ticket.Sale();

            // Act
            var result = await BatchRepository.SaveManyAsync(new[] { ticket }, "direct_save").ConfigureAwait(false);

            // Assert
            Assert.NotNull(result);
            Assert.True(result.Success);
            Assert.Single(result.Results);
            Assert.True(result.Results[0].Succeeded);

            // Verify persisted
            var chunk = await Persistence.ReadSingleBackwardAsync("Direct_Ticket_1").ConfigureAwait(false);
            Assert.NotNull(chunk);
        }

        [Fact]
        public async Task saving_new_aggregate_adds_it_to_tracking()
        {
            // Arrange
            var ticket = Ticket.CreateNew("Direct_Ticket_2");
            ticket.Sale();

            // Act - save the new aggregate
            await BatchRepository.SaveManyAsync(new[] { ticket }, "track_save").ConfigureAwait(false);

            // Now load it - should return the same instance from cache
            var loaded = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Direct_Ticket_2" }).ConfigureAwait(false);

            // Assert - should be the same instance (tracked)
            Assert.Same(ticket, loaded["Direct_Ticket_2"]);
        }

        [Fact]
        public async Task can_save_mix_of_new_and_loaded_aggregates()
        {
            // Arrange
            var loadedTickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Loaded_Ticket" }).ConfigureAwait(false);
            loadedTickets["Loaded_Ticket"].Sale();

            var newTicket = Ticket.CreateNew("New_Ticket");
            newTicket.Sale();

            var aggregatesToSave = new IAggregate[] { loadedTickets["Loaded_Ticket"], newTicket };

            // Act
            var result = await BatchRepository.SaveManyAsync(aggregatesToSave, "mix_save").ConfigureAwait(false);

            // Assert
            Assert.True(result.Success);
            Assert.Equal(2, result.Results.Count);

            var chunk1 = await Persistence.ReadSingleBackwardAsync("Loaded_Ticket").ConfigureAwait(false);
            var chunk2 = await Persistence.ReadSingleBackwardAsync("New_Ticket").ConfigureAwait(false);
            Assert.NotNull(chunk1);
            Assert.NotNull(chunk2);
        }
    }

    public class batch_with_cancellation : BaseBatchRepositoryTest
    {
        [Fact]
        public async Task GetManyByIdAsync_should_respect_cancellation_token()
        {
            // Arrange
            using var cts = new CancellationTokenSource();
            cts.Cancel(); // Cancel immediately

            // Act & Assert
            await Assert.ThrowsAsync<OperationCanceledException>(
                () => BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }, cts.Token)
            ).ConfigureAwait(false);
        }

        [Fact]
        public async Task SaveManyAsync_with_cancelled_token_should_complete_for_empty_batch()
        {
            // Arrange - with empty batch, cancellation doesn't matter since no actual work is done
            using var cts = new CancellationTokenSource();
            cts.Cancel(); // Cancel immediately

            // Act - empty batch returns immediately without checking cancellation
            var result = await BatchRepository.SaveManyAsync(new IAggregate[0], "cancel_op", cancellationToken: cts.Token).ConfigureAwait(false);

            // Assert - empty batch completes even with cancelled token
            Assert.NotNull(result);
            Assert.Empty(result.Results);
        }
    }

    public class batch_snapshots_on_partial_failure : BaseBatchRepositoryTest
    {
        private readonly DefaultSnapshotStore _snapshotStore;

        public batch_snapshots_on_partial_failure()
        {
            _snapshotStore = new DefaultSnapshotStore(new InMemoryPersistence(new InMemoryPersistenceOptions()));
            SnapshotBatchStore = new DefaultSnapshotBatchStore(_snapshotStore);

            // Setup initial data
            Persistence.AppendAsync("Ticket_1", 1, new Changeset(1, new object[] { new TicketSold() })).Wait();
            Persistence.AppendAsync("Ticket_2", 1, new Changeset(1, new object[] { new TicketSold() })).Wait();
        }

        [Fact]
        public async Task snapshots_should_only_be_saved_for_successful_aggregates()
        {
            // Arrange - load in two repositories
            var repo1 = CreateBatchRepository();
            var repo2 = CreateBatchRepository();

            var tickets1 = await repo1.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            var tickets2 = await repo2.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2" }).ConfigureAwait(false);

            // Repo1 modifies Ticket_1 only
            tickets1["Ticket_1"].DoSomething();
            await repo1.SaveManyAsync(tickets1.Values, Guid.NewGuid().ToString()).ConfigureAwait(false);

            // Repo2 tries to modify both - Ticket_1 will fail (concurrency), Ticket_2 will succeed
            tickets2["Ticket_1"].DoSomething();
            tickets2["Ticket_2"].DoSomething();
            var result = await repo2.SaveManyAsync(tickets2.Values, Guid.NewGuid().ToString()).ConfigureAwait(false);

            // Assert - partial failure
            Assert.False(result.Success);

            // Check snapshots - only Ticket_2 should have a snapshot from repo2
            // Note: repo1 also saved a snapshot for Ticket_1 at version 2
            var snap1 = await _snapshotStore.GetAsync("Ticket_1", int.MaxValue).ConfigureAwait(false);
            var snap2 = await _snapshotStore.GetAsync("Ticket_2", int.MaxValue).ConfigureAwait(false);

            // Ticket_1 snapshot should be from repo1 (version 2)
            Assert.NotNull(snap1);
            Assert.Equal(2, snap1.SourceVersion);

            // Ticket_2 snapshot should be from repo2 (version 2)
            Assert.NotNull(snap2);
            Assert.Equal(2, snap2.SourceVersion);
        }
    }

    public class batch_constructor_validation : BaseBatchRepositoryTest
    {
        [Fact]
        public void constructor_should_throw_on_null_factory()
        {
            var multiReader = (IMultiPartitionPersistenceReader)Persistence;
            var enhancedPersistence = (IEnhancedPersistence)Persistence;

            Assert.Throws<ArgumentNullException>(() => new BatchRepository(
                null,
                multiReader,
                enhancedPersistence,
                null
            ));
        }

        [Fact]
        public void constructor_should_throw_on_null_multiReader()
        {
            var enhancedPersistence = (IEnhancedPersistence)Persistence;

            Assert.Throws<ArgumentNullException>(() => new BatchRepository(
                AggregateFactory,
                null,
                enhancedPersistence,
                null
            ));
        }

        [Fact]
        public void constructor_should_throw_on_null_persistence()
        {
            var multiReader = (IMultiPartitionPersistenceReader)Persistence;

            Assert.Throws<ArgumentNullException>(() => new BatchRepository(
                AggregateFactory,
                multiReader,
                null,
                null
            ));
        }

        [Fact]
        public void constructor_should_allow_null_snapshot_store()
        {
            var multiReader = (IMultiPartitionPersistenceReader)Persistence;
            var enhancedPersistence = (IEnhancedPersistence)Persistence;

            // Should not throw
            var repo = new BatchRepository(
                AggregateFactory,
                multiReader,
                enhancedPersistence,
                null
            );

            Assert.NotNull(repo);
        }
    }

    public class batch_concurrent_loading : BaseBatchRepositoryTest
    {
        public batch_concurrent_loading()
        {
            Persistence.AppendAsync("Ticket_1", 1, new Changeset(1, new object[] { new TicketSold() })).Wait();
            Persistence.AppendAsync("Ticket_2", 1, new Changeset(1, new object[] { new TicketSold() })).Wait();
        }

        [Fact]
        public async Task concurrent_GetManyByIdAsync_with_overlapping_ids_should_return_same_instances()
        {
            // Arrange - start two parallel loads with overlapping IDs
            var task1 = BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2" });
            var task2 = BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" });

            // Act
            await Task.WhenAll(task1, task2).ConfigureAwait(false);

            var result1 = await task1;
            var result2 = await task2;

            // Assert - Ticket_1 should be the same instance in both results
            Assert.Same(result1["Ticket_1"], result2["Ticket_1"]);
        }

        [Fact]
        public async Task loading_with_mix_of_cached_and_new_ids()
        {
            // Arrange - first load
            var first = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);

            // Act - second load with mix of cached (Ticket_1) and new (Ticket_2)
            var second = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2" }).ConfigureAwait(false);

            // Assert
            Assert.Same(first["Ticket_1"], second["Ticket_1"]); // Cached
            Assert.NotNull(second["Ticket_2"]); // Newly loaded
            Assert.Equal(1, second["Ticket_2"].Version);
        }
    }
}
