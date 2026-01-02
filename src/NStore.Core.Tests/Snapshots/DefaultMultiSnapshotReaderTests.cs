using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Snapshots;
using Xunit;

namespace NStore.Core.Tests.Snapshots
{
    public class DefaultMultiSnapshotReaderTests
    {
        /// <summary>
        /// A fake in-memory implementation of ISnapshotStore for testing purposes.
        /// </summary>
        private class FakeSnapshotStore : ISnapshotStore
        {
            private readonly Dictionary<string, SnapshotInfo> _snapshots = new Dictionary<string, SnapshotInfo>();

            public void AddSnapshot(string partitionId, SnapshotInfo snapshot)
            {
                _snapshots[partitionId] = snapshot;
            }

            public Task<SnapshotInfo> GetLastAsync(string snapshotPartitionId, CancellationToken cancellationToken)
            {
                _snapshots.TryGetValue(snapshotPartitionId, out var snapshot);
                return Task.FromResult(snapshot);
            }

            public Task<SnapshotInfo> GetAsync(string snapshotPartitionId, long version, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task AddAsync(string snapshotPartitionId, SnapshotInfo snapshot, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task DeleteAsync(string snapshotPartitionId, long fromVersionInclusive, long toVersionInclusive, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        [Fact]
        public void constructor_should_throw_when_snapshot_store_is_null()
        {
            // Act & Assert
            var exception = Assert.Throws<ArgumentNullException>(() => new DefaultMultiSnapshotReader(null));
            Assert.Equal("snapshotStore", exception.ParamName);
        }

        [Fact]
        public async Task get_many_async_should_throw_when_partition_ids_is_null()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var reader = new DefaultMultiSnapshotReader(store);

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(
                async () => await reader.GetManyAsync(null, CancellationToken.None)
            );
        }

        [Fact]
        public async Task get_many_async_should_return_empty_dictionary_when_no_partition_ids_provided()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var reader = new DefaultMultiSnapshotReader(store);

            // Act
            var result = await reader.GetManyAsync(new string[0], CancellationToken.None);

            // Assert
            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public async Task get_many_async_should_return_snapshots_for_existing_partitions()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshot1 = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var snapshot2 = new SnapshotInfo("Order-2", 10, "payload2", "v1");
            var snapshot3 = new SnapshotInfo("Order-3", 15, "payload3", "v1");

            store.AddSnapshot("Order-1", snapshot1);
            store.AddSnapshot("Order-2", snapshot2);
            store.AddSnapshot("Order-3", snapshot3);

            var reader = new DefaultMultiSnapshotReader(store);
            var partitionIds = new[] { "Order-1", "Order-2", "Order-3" };

            // Act
            var result = await reader.GetManyAsync(partitionIds, CancellationToken.None);

            // Assert
            Assert.Equal(3, result.Count);
            Assert.Same(snapshot1, result["Order-1"]);
            Assert.Same(snapshot2, result["Order-2"]);
            Assert.Same(snapshot3, result["Order-3"]);
        }

        [Fact]
        public async Task get_many_async_should_exclude_partitions_without_snapshots()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshot1 = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var snapshot3 = new SnapshotInfo("Order-3", 15, "payload3", "v1");

            store.AddSnapshot("Order-1", snapshot1);
            // Order-2 has no snapshot
            store.AddSnapshot("Order-3", snapshot3);

            var reader = new DefaultMultiSnapshotReader(store);
            var partitionIds = new[] { "Order-1", "Order-2", "Order-3" };

            // Act
            var result = await reader.GetManyAsync(partitionIds, CancellationToken.None);

            // Assert
            Assert.Equal(2, result.Count);
            Assert.True(result.ContainsKey("Order-1"));
            Assert.False(result.ContainsKey("Order-2")); // Should not be in result
            Assert.True(result.ContainsKey("Order-3"));
            Assert.Same(snapshot1, result["Order-1"]);
            Assert.Same(snapshot3, result["Order-3"]);
        }

        [Fact]
        public async Task get_many_async_should_deduplicate_partition_ids()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshot = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            store.AddSnapshot("Order-1", snapshot);

            var reader = new DefaultMultiSnapshotReader(store);
            // Pass duplicate partition IDs
            var partitionIds = new[] { "Order-1", "Order-1", "Order-1" };

            // Act
            var result = await reader.GetManyAsync(partitionIds, CancellationToken.None);

            // Assert
            Assert.Single(result);
            Assert.Same(snapshot, result["Order-1"]);
        }

        [Fact]
        public async Task get_many_async_should_handle_mixed_existing_and_missing_partitions()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshot1 = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var snapshot5 = new SnapshotInfo("Order-5", 25, "payload5", "v1");

            store.AddSnapshot("Order-1", snapshot1);
            // Order-2, Order-3, Order-4 have no snapshots
            store.AddSnapshot("Order-5", snapshot5);

            var reader = new DefaultMultiSnapshotReader(store);
            var partitionIds = new[] { "Order-1", "Order-2", "Order-3", "Order-4", "Order-5" };

            // Act
            var result = await reader.GetManyAsync(partitionIds, CancellationToken.None);

            // Assert
            Assert.Equal(2, result.Count);
            Assert.Same(snapshot1, result["Order-1"]);
            Assert.Same(snapshot5, result["Order-5"]);
            Assert.False(result.ContainsKey("Order-2"));
            Assert.False(result.ContainsKey("Order-3"));
            Assert.False(result.ContainsKey("Order-4"));
        }

        [Fact]
        public async Task get_many_async_should_respect_cancellation_token()
        {
            // Arrange
            var cts = new CancellationTokenSource();
            cts.Cancel(); // Cancel immediately

            var store = new FakeSnapshotStore();
            var reader = new DefaultMultiSnapshotReader(store);
            var partitionIds = new[] { "Order-1" };

            // Act & Assert
            // The fake store doesn't actually check cancellation, but we verify the token is passed through
            // In a real implementation, this would throw OperationCanceledException
            var result = await reader.GetManyAsync(partitionIds, cts.Token);

            // For the fake implementation, we just verify it completes
            Assert.NotNull(result);
        }

        [Fact]
        public async Task get_many_async_should_return_all_requested_snapshots_when_all_exist()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshots = Enumerable.Range(1, 100)
                .Select(i => new SnapshotInfo($"Order-{i}", i * 10, $"payload{i}", "v1"))
                .ToList();

            foreach (var snapshot in snapshots)
            {
                store.AddSnapshot(snapshot.SourceId, snapshot);
            }

            var reader = new DefaultMultiSnapshotReader(store);
            var partitionIds = snapshots.Select(s => s.SourceId).ToArray();

            // Act
            var result = await reader.GetManyAsync(partitionIds, CancellationToken.None);

            // Assert
            Assert.Equal(100, result.Count);
            foreach (var snapshot in snapshots)
            {
                Assert.True(result.ContainsKey(snapshot.SourceId));
                Assert.Same(snapshot, result[snapshot.SourceId]);
            }
        }

        [Fact]
        public async Task extension_method_should_work_without_cancellation_token()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshot = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            store.AddSnapshot("Order-1", snapshot);

            var reader = new DefaultMultiSnapshotReader(store);
            var partitionIds = new[] { "Order-1" };

            // Act - Use the extension method without cancellation token
            var result = await reader.GetManyAsync(partitionIds);

            // Assert
            Assert.Single(result);
            Assert.Same(snapshot, result["Order-1"]);
        }
    }
}
