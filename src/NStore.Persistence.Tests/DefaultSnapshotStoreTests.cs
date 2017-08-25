using System;
using System.Threading.Tasks;
using NStore.Persistence;
using NStore.Snapshots;
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
            var snapshot = await _snapshots.GetAsync("no-one", 1);
            Assert.Null(snapshot);
        }

        [Fact]
        public async Task empty_snapshot_is_not_persisted()
        {
            var nullSnapshot = new SnapshotInfo("empty", 0, null, 0);
            await _snapshots.AddAsync("empty", nullSnapshot);

            var tape = new Recorder();
            await Store.ReadForwardAsync("empty", 0, tape);

            Assert.True(tape.IsEmpty);
        }

        [Fact]
        public async Task snapshot_data_should_be_different_across_write_and_reads()
        {
            var input = new SnapshotInfo("Aggregate_1", 1, new State(), 1);

            await _snapshots.AddAsync("Aggregate_1", input);
            var output = await _snapshots.GetAsync("Aggregate_1", Int32.MaxValue);

            Assert.NotSame(input.Payload, output.Payload);
        }

        [Fact]
        public async Task snapshots_can_be_deleted()
        {
            var input = new SnapshotInfo("Aggregate_1", 1, new State(), 1);
            await _snapshots.AddAsync("Aggregate_1", input);

            await _snapshots.DeleteAsync("Aggregate_1");

            var tape = new Recorder();
            await Store.ReadForwardAsync("Aggregate_1", 0, tape);

            Assert.True(tape.IsEmpty);
        }
    }
}