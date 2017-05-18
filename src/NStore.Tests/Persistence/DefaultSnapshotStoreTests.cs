using System;
using NStore.InMemory;
using NStore.Raw;
using NStore.SnapshotStore;
using NStore.Tests.Support;
using Xunit;

namespace NStore.Tests.Persistence
{
    //@@REVIEW Use real implementation on all persistence providers or just a mocking fx?
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
            _rawStore = new RawStoreInterceptor(
                new InMemoryRawStore(cloneFunc: Clone)
            );
            _snapshots = new DefaultSnapshotStore(_rawStore);
        }

        private object Clone(object source)
        {
            if (source == null)
                return null;

            var si = (SnapshotInfo)source;
            return new SnapshotInfo(
                si.AggregateId,
                si.AggregateVersion,
                new State((State)si.Data),
                si.SnapshotVersion
            );
        }

        [Fact]
        public async void loading_missing_snapshot_should_return_empty()
        {
            var snapshot = await _snapshots.Get("no-one", 1);
            Assert.Null(snapshot);
        }

        [Fact]
        public async void empty_snapshot_is_not_persisted()
        {
            var nullSnapshot = new SnapshotInfo("empty", 0, null, 0);
            await _snapshots.Add("empty", nullSnapshot);

            var tape = new Tape();
            await _rawStore.ScanPartitionAsync("empty", 0, ScanDirection.Forward, tape);

            Assert.True(tape.IsEmpty);
        }

        [Fact]
        public async void snapshot_data_should_be_different_across_write_and_reads()
        {
            var input = new SnapshotInfo("Aggregate_1", 1, new State(), 1);

            await _snapshots.Add("Aggregate_1", input);
            var output = await _snapshots.Get("Aggregate_1", Int32.MaxValue);

            Assert.NotSame(input.Data, output.Data);
        }

        [Fact]
        public async void snapshots_can_be_deleted()
        {
            var input = new SnapshotInfo("Aggregate_1", 1, new State(), 1);
            await _snapshots.Add("Aggregate_1", input);

            await _snapshots.Remove("Aggregate_1");

            var tape = new Tape();
            await _rawStore.ScanPartitionAsync("Aggregate_1", 0, ScanDirection.Forward, tape);

            Assert.True(tape.IsEmpty);
        }
    }
}