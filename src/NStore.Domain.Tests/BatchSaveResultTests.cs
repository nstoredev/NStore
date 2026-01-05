using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace NStore.Domain.Tests
{
    public class BatchSaveResultTests : BaseBatchRepositoryTest
    {
        [Fact]
        public async Task SaveManyAsync_should_return_populated_result_for_single_changed_aggregate()
        {
            // Arrange
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();

            // Act
            var result = await BatchRepository.SaveManyAsync(tickets.Values, "tdd_op").ConfigureAwait(false);

            // Assert - TDD expectation: result should contain one successful AggregateSaveResult with non-null Chunk
            Assert.NotNull(result);
            Assert.True(result.Success, "Expected overall success");
            Assert.False(result.HasFailures, "Expected no failures");
            Assert.NotNull(result.Results);
            Assert.Single(result.Results);
            var res = result.Results[0];
            Assert.Equal("Ticket_1", res.AggregateId);
            Assert.True(res.Succeeded, "Expected aggregate to be succeeded");
            Assert.Null(res.FailureException);
            Assert.NotNull(res.Chunk);
            Assert.Equal("Ticket_1", res.Chunk.PartitionId);
        }

        [Fact]
        public async Task SaveManyAsync_should_return_multiple_results_for_multiple_aggregates()
        {
            // Arrange
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2", "Ticket_3" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();
            tickets["Ticket_2"].Sale();
            tickets["Ticket_3"].Sale();

            // Act
            var result = await BatchRepository.SaveManyAsync(tickets.Values, "batch_op").ConfigureAwait(false);

            // Assert - TDD expectation: 3 successful results
            Assert.NotNull(result);
            Assert.True(result.Success);
            Assert.False(result.HasFailures);
            Assert.Equal(3, result.Results.Count);
            Assert.All(result.Results, r => Assert.True(r.Succeeded));
            Assert.All(result.Results, r => Assert.NotNull(r.Chunk));
            Assert.All(result.Results, r => Assert.Null(r.FailureException));
        }

        [Fact]
        public async Task SaveManyAsync_should_return_empty_result_when_no_changes()
        {
            // Arrange
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            // No changes made

            // Act
            var result = await BatchRepository.SaveManyAsync(tickets.Values, "no_change_op").ConfigureAwait(false);

            // Assert - TDD expectation: empty result (no write jobs generated)
            Assert.NotNull(result);
            Assert.True(result.Success);
            Assert.False(result.HasFailures);
            Assert.Empty(result.Results);
        }

        [Fact]
        public async Task SaveManyAsync_should_report_concurrency_failure_in_result()
        {
            // Arrange - set up a concurrency conflict
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            tickets["Ticket_1"].Sale();
            await BatchRepository.SaveManyAsync(tickets.Values, "first").ConfigureAwait(false);

            var repo2 = CreateBatchRepository();
            var tickets1 = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);
            var tickets2 = await repo2.GetManyByIdAsync<Ticket>(new[] { "Ticket_1" }).ConfigureAwait(false);

            tickets1["Ticket_1"].DoSomething();
            await BatchRepository.SaveManyAsync(tickets1.Values, "second").ConfigureAwait(false);

            tickets2["Ticket_1"].DoSomething();

            // Act - second repo saves with stale version
            var result = await repo2.SaveManyAsync(tickets2.Values, "third").ConfigureAwait(false);

            // Assert - TDD expectation: result should indicate failure with exception details
            Assert.NotNull(result);
            Assert.False(result.Success, "Expected failure due to concurrency");
            Assert.True(result.HasFailures);
            Assert.Single(result.Results);
            var res = result.Results[0];
            Assert.Equal("Ticket_1", res.AggregateId);
            Assert.False(res.Succeeded, "Expected aggregate save to fail");
            Assert.NotNull(res.FailureException);
            Assert.Null(res.Chunk); // No chunk on failure
        }

        [Fact]
        public async Task SaveManyAsync_should_report_partial_success_with_mixed_results()
        {
            // Arrange - create initial state for 3 aggregates
            var tickets = await BatchRepository.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2", "Ticket_3" }).ConfigureAwait(false);
            foreach (var t in tickets.Values) t.Sale();
            await BatchRepository.SaveManyAsync(tickets.Values, "init").ConfigureAwait(false);

            // Create two repos with same aggregates
            var repo1 = CreateBatchRepository();
            var repo2 = CreateBatchRepository();

            var tickets1 = await repo1.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2" }).ConfigureAwait(false);
            var tickets2 = await repo2.GetManyByIdAsync<Ticket>(new[] { "Ticket_1", "Ticket_2", "Ticket_3" }).ConfigureAwait(false);

            // Repo1 modifies Ticket_1 and Ticket_2
            tickets1["Ticket_1"].DoSomething();
            tickets1["Ticket_2"].DoSomething();
            await repo1.SaveManyAsync(tickets1.Values, "conflict_setup").ConfigureAwait(false);

            // Repo2 tries to modify all three (Ticket_1 and Ticket_2 will conflict, Ticket_3 should succeed)
            tickets2["Ticket_1"].DoSomething();
            tickets2["Ticket_2"].DoSomething();
            tickets2["Ticket_3"].DoSomething();

            // Act
            var result = await repo2.SaveManyAsync(tickets2.Values, "partial").ConfigureAwait(false);

            // Assert - TDD expectation: partial success/failure
            Assert.NotNull(result);
            Assert.False(result.Success, "Expected partial failure");
            Assert.True(result.HasFailures);
            Assert.Equal(3, result.Results.Count);

            var failed = result.Results.Where(r => !r.Succeeded).ToList();
            var succeeded = result.Results.Where(r => r.Succeeded).ToList();

            Assert.Equal(2, failed.Count); // Ticket_1 and Ticket_2 failed
            Assert.Single(succeeded); // Ticket_3 succeeded

            Assert.All(failed, r => Assert.NotNull(r.FailureException));
            Assert.All(failed, r => Assert.Null(r.Chunk));

            Assert.All(succeeded, r => Assert.Null(r.FailureException));
            Assert.All(succeeded, r => Assert.NotNull(r.Chunk));
        }
    }
}
