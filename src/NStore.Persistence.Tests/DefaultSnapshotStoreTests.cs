using System;
using System.Threading.Tasks;
using NStore.Persistence;
using NStore.SnapshotStore;
using Xunit;

namespace NStore.Persistence.Tests
{
    public class State
    {
        public State()
        {
        }

        public State(State source)
        {
        }
    }
    
    public class DefaultSnapshotStoreTests : BasePersistenceTest
    {
        private readonly ISnapshotStore _snapshots;

        public DefaultSnapshotStoreTests()
        {
            _snapshots = new DefaultSnapshotStore(Store);
        }

        [Fact]
        public async Task loading_missing_snapshot_should_return_empty()
        {
            var snapshot = await _snapshots.Get("no-one", 1);
            Assert.Null(snapshot);
        }

        [Fact]
        public async Task empty_snapshot_is_not_persisted()
        {
            var nullSnapshot = new SnapshotInfo("empty", 0, null, 0);
            await _snapshots.Add("empty", nullSnapshot);

            var tape = new Recorder();
            await Store.ReadPartitionForward("empty", 0, tape);

            Assert.True(tape.IsEmpty);
        }

        [Fact]
        public async Task snapshot_data_should_be_different_across_write_and_reads()
        {
            var input = new SnapshotInfo("Aggregate_1", 1, new State(), 1);

            await _snapshots.Add("Aggregate_1", input);
            var output = await _snapshots.Get("Aggregate_1", Int32.MaxValue);

            Assert.NotSame(input.Data, output.Data);
        }

        [Fact]
        public async Task snapshots_can_be_deleted()
        {
            var input = new SnapshotInfo("Aggregate_1", 1, new State(), 1);
            await _snapshots.Add("Aggregate_1", input);

            await _snapshots.Remove("Aggregate_1");

            var tape = new Recorder();
            await Store.ReadPartitionForward("Aggregate_1", 0, tape);

            Assert.True(tape.IsEmpty);
        }
    }
}