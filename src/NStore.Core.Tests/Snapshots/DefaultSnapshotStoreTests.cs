using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using Xunit;

namespace NStore.Core.Tests.Snapshots
{
    public class DefaultSnapshotStoreTests
    {
        [Fact]
        public async Task add_many_async_should_ignore_duplicated_index_results_from_batch_append()
        {
            var persistence = new RecordingEnhancedPersistence(queue =>
            {
                queue[0].Failed(WriteJob.WriteResult.DuplicatedIndex);
                queue[1].Succeeded();
            });

            var snapshotStore = new DefaultSnapshotStore(persistence);
            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                ["A"] = new SnapshotInfo("A", 1, "payload-a", "v1"),
                ["B"] = new SnapshotInfo("B", 1, "payload-b", "v1")
            };

            await snapshotStore.AddManyAsync(snapshots, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, persistence.AppendBatchCallCount);
        }

        [Fact]
        public async Task add_many_async_should_silently_ignore_non_duplicate_failures()
        {
            var persistence = new RecordingEnhancedPersistence(queue =>
            {
                queue[0].Failed(WriteJob.WriteResult.Failed);
            });

            var snapshotStore = new DefaultSnapshotStore(persistence);
            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                ["A"] = new SnapshotInfo("A", 1, "payload-a", "v1")
            };

            // Best-effort: should complete without throwing even when individual jobs fail.
            // Missing snapshots will be rebuilt from the event stream on next load.
            await snapshotStore.AddManyAsync(snapshots, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, persistence.AppendBatchCallCount);
        }

        private sealed class RecordingEnhancedPersistence : NullPersistence, IEnhancedPersistence
        {
            private readonly Action<WriteJob[]> _onAppendBatch;
            private int _appendBatchCallCount;

            public RecordingEnhancedPersistence(Action<WriteJob[]> onAppendBatch)
            {
                _onAppendBatch = onAppendBatch ?? throw new ArgumentNullException(nameof(onAppendBatch));
            }

            public int AppendBatchCallCount => _appendBatchCallCount;

            public Task AppendBatchAsync(WriteJob[] queue, CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _appendBatchCallCount);
                _onAppendBatch(queue);
                return Task.CompletedTask;
            }
        }
    }
}
