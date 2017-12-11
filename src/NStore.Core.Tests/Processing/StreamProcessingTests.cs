using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Processing;
using NStore.Core.Snapshots;
using NStore.Core.Streams;
using Xunit;

namespace NStore.Core.Tests.Processing
{
    public class CounterProcessor
    {
        public int Counter { get; private set; }

        public object Process(object input)
        {
            Counter++;
            return input;
        }
    }

    public class Sum
    {
        public int Total { get; set; }

        private void On(ValueCollected data)
        {
            this.Total += data.Value;
        }
    }

    public class SumAsync
    {
        public int Total { get; private set; }

        private async Task On(ValueCollected data)
        {
            await Task.Delay(10);
            this.Total += data.Value;
        }
    }

    public class ValueCollected
    {
        public ValueCollected(int value)
        {
            Value = value;
        }

        public int Value { get; private set; }
    }

    public class StreamProcessingTests
    {
        private readonly IPersistence _persistence;
        private readonly StreamsFactory _streams;
        private readonly ISnapshotStore _snapshots;

        public StreamProcessingTests()
        {
            _persistence = new InMemoryPersistence();
            _streams = new StreamsFactory(_persistence);
            _snapshots = new DefaultSnapshotStore(new InMemoryPersistence());
        }

        private async Task<IStream> CreateStream(string streamId)
        {
            var stream = _streams.Open(streamId);
            for (int value = 1; value <= 10; value++)
            {
                await stream.AppendAsync(new ValueCollected(value)).ConfigureAwait(false);
            }

            return stream;
        }

        [Fact]
        public async Task should_sum_all_values()
        {
            var sequence = await CreateStream("sequence_1").ConfigureAwait(false);
            var result = await sequence.Fold().RunAsync<Sum>().ConfigureAwait(false);
            Assert.Equal(55, result.Total);
        }

        [Fact]
        public async Task should_sum_all_values_async()
        {
            var sequence = await CreateStream("sequence_1").ConfigureAwait(false);
            var result = await sequence.Fold().RunAsync<SumAsync>().ConfigureAwait(false);
            Assert.Equal(55, result.Total);
        }

        [Fact]
        public async Task should_sum_fist_two_values()
        {
            var sequence = await CreateStream("sequence_1").ConfigureAwait(false);
            var result = await sequence
                .Fold()
                .ToIndex(2)
                .RunAsync<Sum>().ConfigureAwait(false);
            Assert.Equal(3, result.Total);
        }

        [Fact]
        public async Task should_load_snapshots()
        {
            await _snapshots.AddAsync("sequence_1/Sum", new SnapshotInfo(
                "sequence_1", 9, new Sum {Total = 45}, "1"
            )).ConfigureAwait(false);

            var sequence = await CreateStream("sequence_1").ConfigureAwait(false);
            var result = await sequence
                .Fold()
                .WithCache(_snapshots)
                .RunAsync<Sum>().ConfigureAwait(false);

            Assert.Equal(45 + 10, result.Total);
        }

        /// <summary>
        /// not 100% sure about this behaviour. 
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task should_return_snapshotted_value_on_empty_stream()
        {
            await _snapshots.AddAsync("sequence_1/Sum", new SnapshotInfo(
                "sequence_1", 11, new Sum {Total = 1}, "1"
            )).ConfigureAwait(false);

            var sequence = _streams.Open("sequence_1");
            var result = await sequence
                .Fold()
                .WithCache(_snapshots)
                .RunAsync<Sum>().ConfigureAwait(false);

            Assert.Equal(1, result.Total);
        }

        [Fact]
        public async Task should_ignore_snapshotted_value_ahead_of_requested_version()
        {
            await _snapshots.AddAsync("sequence_1/Sum", new SnapshotInfo(
                "sequence_1", 11, new Sum {Total = 1}, "1"
            )).ConfigureAwait(false);

            var sequence = _streams.Open("sequence_1");
            var result = await sequence
                .Fold()
                .ToIndex(10)
                .WithCache(_snapshots)
                .RunAsync<Sum>().ConfigureAwait(false);

            Assert.Equal(0, result.Total);
        }

        [Fact]
        public async Task should_add_snapshot()
        {
            var sequence = await CreateStream("sequence_1").ConfigureAwait(false);
            var result = await sequence
                .Fold()
                .WithCache(_snapshots)
                .RunAsync<Sum>().ConfigureAwait(false);

            var snapshotted = await _snapshots.GetLastAsync("sequence_1/Sum").ConfigureAwait(false);
            Assert.NotNull(snapshotted);
            Assert.Equal(result.Total, ((Sum) snapshotted.Payload).Total);
        }

        [Fact]
        public async Task should_skip_holes()
        {
            var sequence = await CreateStream("sequence_1").ConfigureAwait(false);
            await _persistence.DeleteAsync(sequence.Id, 2, 9).ConfigureAwait(false);

            var result = await sequence
                .Fold()
                .ToIndex(10)
                .WithCache(_snapshots)
                .RunAsync<Sum>().ConfigureAwait(false);

            Assert.Equal(11, result.Total);
        }

        [Fact]
        public async Task should_signal_holes()
        {
            var sequence = await CreateStream("sequence_1").ConfigureAwait(false);
            await _persistence.DeleteAsync(sequence.Id, 1, 1).ConfigureAwait(false);
            await _persistence.DeleteAsync(sequence.Id, 3, 4).ConfigureAwait(false);
            await _persistence.DeleteAsync(sequence.Id, 6, 7).ConfigureAwait(false);

            var missing = new List<Tuple<long, long>>();

            var result = await sequence
                .Fold()
                .ToIndex(10)
                .OnMissing((from, to) =>
                {
                    missing.Add(new Tuple<long, long>(from, to));
                    return true;
                })
                .WithCache(_snapshots)
                .RunAsync<Sum>().ConfigureAwait(false);

            Assert.Equal(2 + 5 + 8 + 9 + 10, result.Total);
            Assert.Collection(missing,
                l => Assert.Equal(new Tuple<long, long>(1, 1), l),
                l => Assert.Equal(new Tuple<long, long>(3, 4), l),
                l => Assert.Equal(new Tuple<long, long>(6, 7), l)
            );
        }

        [Fact]
        public async Task should_fold_using_lambda()
        {
            var sequence = await CreateStream("sequence_1").ConfigureAwait(false);
            var result = await sequence
                .Fold()
                .RunAsync<CounterProcessor>((c, o) => c.Process(o));
            
            Assert.Equal(10,result.Counter);
        }
    }
}