using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using Xunit;

namespace NStore.Core.Tests.InMemory
{
    public class InMemoryPersistenceTests
    {
        private class TestNetworkSimulator : INetworkSimulator
        {
            public int WaitCountFast;
            public int WaitCount;

            public Task<long> WaitFast()
            {
                Interlocked.Increment(ref WaitCountFast);
                return Task.FromResult(0L);
            }

            public Task<long> Wait()
            {
                Interlocked.Increment(ref WaitCount);
                return Task.FromResult(0L);
            }
        }

        private class Payload
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task Ctor_WithNetworkSimulator_IsUsedByOperations()
        {
            var simulator = new TestNetworkSimulator();
            var persistence = new InMemoryPersistence(simulator);

            // Append should call Wait on the provided simulator
            var chunk = await persistence.AppendAsync("p1", 0, new Payload { Name = "a" }, null, CancellationToken.None);
            Assert.NotNull(chunk);
            Assert.True(simulator.WaitCount > 0 || simulator.WaitCountFast > 0);
        }

        [Fact]
        public async Task Ctor_WithCloneFunc_UsesProvidedCloneFunction()
        {
            Func<object, object> cloneFunc = o => new Payload { Name = "cloned-" + ((Payload)o).Name };

            var persistence = new InMemoryPersistence(cloneFunc);

            var original = new Payload { Name = "x" };
            await persistence.AppendAsync("p2", 0, original, null, CancellationToken.None);

            var read = await persistence.ReadOneAsync(1, CancellationToken.None);
            var payload = Assert.IsType<Payload>(read.Payload);
            // clone func is applied both during Append (payload cloned into chunk) and when chunks are cloned on read,
            // so final payload observed by callers will have clone applied twice.
            Assert.Equal("cloned-cloned-x", payload.Name);
            // ensure we did not mutate original
            Assert.Equal("x", original.Name);
        }

        [Fact]
        public async Task PartitionIds_ContainsPartitions_And_ExcludesEmpty()
        {
            var persistence = new InMemoryPersistence();

            // first write succeeds
            await persistence.AppendAsync("p3", 0, "a", null, CancellationToken.None);

            // second write with same index triggers DuplicateStreamIndexException and writes an empty chunk
            await Assert.ThrowsAsync<DuplicateStreamIndexException>(() => persistence.AppendAsync("p3", 0, "b", null, CancellationToken.None));

            var ids = persistence.PartitionIds.ToList();
            Assert.Contains("p3", ids);
            Assert.DoesNotContain("::empty", ids);
        }

        private class DummySubscription : ISubscription
        {
            public Task OnStartAsync(long indexOrPosition) => Task.CompletedTask;
            public Task<bool> OnNextAsync(IChunk chunk) => Task.FromResult(true);
            public Task CompletedAsync(long indexOrPosition) => Task.CompletedTask;
            public Task StoppedAsync(long indexOrPosition) => Task.CompletedTask;
            public Task OnErrorAsync(long indexOrPosition, Exception ex) => Task.CompletedTask;
        }

        [Fact]
        public async Task ReadForwardMultiplePartitionsAsync_NullList_ThrowsArgumentNull()
        {
            var persistence = new InMemoryPersistence();
            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                persistence.ReadForwardMultiplePartitionsAsync(null, 0, new DummySubscription(), long.MaxValue, CancellationToken.None));
        }
    }

    /// <summary>
    /// Tests for InMemoryPersistence optimizations - see GitHub issue #124
    /// </summary>
    public class InMemoryPersistenceMultiPartitionTests
    {
        private readonly InMemoryPersistence _persistence;

        public InMemoryPersistenceMultiPartitionTests()
        {
            _persistence = new InMemoryPersistence();
        }

        [Fact]
        public async Task read_multiple_partitions_should_return_only_existing_partitions()
        {
            // Arrange - create only some of the partitions we'll request
            await _persistence.AppendAsync("partition-a", 1, "payload-a1", "op1", CancellationToken.None);
            await _persistence.AppendAsync("partition-a", 2, "payload-a2", "op2", CancellationToken.None);
            await _persistence.AppendAsync("partition-b", 1, "payload-b1", "op3", CancellationToken.None);

            var recorder = new Recorder();

            // Act - request existing and non-existing partitions
            await _persistence.ReadForwardMultiplePartitionsAsync(
                new[] { "partition-a", "partition-nonexistent", "partition-b", "another-missing" },
                1,
                recorder,
                long.MaxValue,
                CancellationToken.None
            );

            // Assert - should only return chunks from existing partitions
            Assert.Equal(3, recorder.Length);
        }

        [Fact]
        public async Task read_multiple_partitions_with_all_nonexistent_should_return_empty()
        {
            // Arrange - create a partition that won't be queried
            await _persistence.AppendAsync("existing-partition", 1, "payload", "op1", CancellationToken.None);

            var recorder = new Recorder();

            // Act - request only non-existing partitions
            await _persistence.ReadForwardMultiplePartitionsAsync(
                new[] { "nonexistent-a", "nonexistent-b", "nonexistent-c" },
                1,
                recorder,
                long.MaxValue,
                CancellationToken.None
            );

            // Assert - should return no chunks
            Assert.Equal(0, recorder.Length);
        }

        [Fact]
        public async Task read_multiple_partitions_preserves_partition_order()
        {
            // Arrange - create partitions
            await _persistence.AppendAsync("partition-z", 1, "z-payload", "op1", CancellationToken.None);
            await _persistence.AppendAsync("partition-a", 1, "a-payload", "op2", CancellationToken.None);
            await _persistence.AppendAsync("partition-m", 1, "m-payload", "op3", CancellationToken.None);

            var recorder = new Recorder();

            // Act - request in specific order
            await _persistence.ReadForwardMultiplePartitionsAsync(
                new[] { "partition-a", "partition-z", "partition-m" },
                1,
                recorder,
                long.MaxValue,
                CancellationToken.None
            );

            // Assert - should return chunks in partition request order
            var chunks = recorder.Chunks.ToList();
            Assert.Equal(3, recorder.Length);
            Assert.Equal("partition-a", chunks[0].PartitionId);
            Assert.Equal("partition-z", chunks[1].PartitionId);
            Assert.Equal("partition-m", chunks[2].PartitionId);
        }
    }
}
