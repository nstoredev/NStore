using NStore.Core.Persistence;
using Xunit;

namespace NStore.Domain.Tests
{
    public class AggregateSaveResultTests
    {
        private class StubChunk : IChunk
        {
            public long Position { get; set; }
            public string PartitionId { get; set; }
            public long Index { get; set; }
            public object Payload { get; set; }
            public string OperationId { get; set; }
        }

        [Fact]
        public void Unchanged_should_create_successful_result_with_unchanged_outcome()
        {
            var result = AggregateSaveResult.Unchanged("agg-1");

            Assert.Equal("agg-1", result.AggregateId);
            Assert.True(result.Succeeded);
            Assert.Null(result.Chunk);
            Assert.Equal(AggregateSaveOutcome.Unchanged, result.Outcome);
        }

        [Fact]
        public void InvariantFailure_should_create_failed_result_with_invariant_failure_outcome()
        {
            var result = AggregateSaveResult.InvariantFailure("agg-2");

            Assert.Equal("agg-2", result.AggregateId);
            Assert.False(result.Succeeded);
            Assert.Null(result.Chunk);
            Assert.Equal(AggregateSaveOutcome.InvariantFailure, result.Outcome);
        }

        [Fact]
        public void Committed_should_create_successful_result_with_chunk()
        {
            var chunk = new StubChunk { PartitionId = "agg-3", Index = 1 };

            var result = AggregateSaveResult.Committed("agg-3", chunk);

            Assert.Equal("agg-3", result.AggregateId);
            Assert.True(result.Succeeded);
            Assert.Same(chunk, result.Chunk);
            Assert.Null(result.Outcome);
        }

        [Fact]
        public void Concurrency_should_create_failed_result_with_concurrency_outcome()
        {
            var result = AggregateSaveResult.Concurrency("agg-4");

            Assert.Equal("agg-4", result.AggregateId);
            Assert.False(result.Succeeded);
            Assert.Null(result.Chunk);
            Assert.Equal(AggregateSaveOutcome.Concurrency, result.Outcome);
        }

        [Fact]
        public void DuplicatedOperation_should_create_successful_result_with_chunk()
        {
            var chunk = new StubChunk { PartitionId = "agg-5", Index = 2 };

            var result = AggregateSaveResult.DuplicatedOperation("agg-5", chunk);

            Assert.Equal("agg-5", result.AggregateId);
            Assert.True(result.Succeeded);
            Assert.Same(chunk, result.Chunk);
            Assert.Equal(AggregateSaveOutcome.DuplicatedOperation, result.Outcome);
        }

        [Fact]
        public void GenericFailure_should_create_failed_result_with_generic_failure_outcome()
        {
            var result = AggregateSaveResult.GenericFailure("agg-6");

            Assert.Equal("agg-6", result.AggregateId);
            Assert.False(result.Succeeded);
            Assert.Null(result.Chunk);
            Assert.Equal(AggregateSaveOutcome.GenericFailure, result.Outcome);
        }

        [Fact]
        public void DuplicatedPosition_should_create_failed_result_with_duplicated_position_outcome()
        {
            var result = AggregateSaveResult.DuplicatedPosition("agg-7");

            Assert.Equal("agg-7", result.AggregateId);
            Assert.False(result.Succeeded);
            Assert.Null(result.Chunk);
            Assert.Equal(AggregateSaveOutcome.DuplicatedPosition, result.Outcome);
        }
    }
}
