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
}
