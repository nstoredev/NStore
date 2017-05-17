using System;
using NStore.InMemory;
using NStore.Raw;
using NStore.SnapshotStore;
using Xunit;

namespace NStore.Tests.Persistence
{
    public class DefaultSnapshotStoreTests
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

        private readonly IRawStore _rawStore;
        private readonly ISnapshotStore _snapshots;

        public DefaultSnapshotStoreTests()
        {
            _rawStore = new InMemoryRawStore(cloneFunc: Clone);
            _snapshots = new DefaultSnapshotStore(_rawStore);
        }

        private object Clone(object source)
        {
            if (source == null)
                return null;
            
            var si = (SnapshotInfo) source;
            return new SnapshotInfo(si.Version, new State((State)si.Data));
        }

        [Fact]
        public async void loading_missing_snapshot_should_return_empty()
        {
            var snapshot = await _snapshots.Get("no-one", 1);
            Assert.Same(SnapshotInfo.Empty, snapshot);
        }

        [Fact]
        public async void empty_snapshot_is_not_persisted()
        {
            await _snapshots.Add("empty", SnapshotInfo.Empty);

            var tape = new Tape();
            await _rawStore.ScanPartitionAsync("empty", 0, ScanDirection.Forward, tape);

            Assert.True(tape.IsEmpty);
        }

        [Fact]
        public async void snapshot_data_should_be_different_across_write_and_reads()
        {
            var input = new SnapshotInfo(0, new State());

            await _snapshots.Add("Aggregate_1", input);
            var output = await _snapshots.Get("Aggregate_1", Int32.MaxValue);

            Assert.NotSame(input.Data, output.Data);
        }
    }
}
