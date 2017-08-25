using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NStore.InMemory;
using NStore.Processing;
using NStore.Snapshots;
using NStore.Streams;
using Xunit;

namespace NStore.Tests.Processing
{
    public class Sum : PayloadProcessor
    {
        public int Total { get; private set; }

        private void On(ValueCollected data)
        {
            this.Total += data.Value;
        }
    }

    public class SumAsync : AsyncPayloadProcessor
    {
        public int Total { get; private set; }

        private Task On(ValueCollected data)
        {
            this.Total += data.Value;
            return Task.CompletedTask;
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
        readonly StreamsFactory _streams = new StreamsFactory(new InMemoryPersistence());
        readonly ISnapshotStore _snapshots = new DefaultSnapshotStore(new InMemoryPersistence());

        private async Task<IStream> CreateStream(string streamId)
        {
            var stream = _streams.Open(streamId);
            for (int value = 1; value <= 10; value++)
            {
                await stream.AppendAsync(new ValueCollected(value));
            }
            return stream;
        }

        [Fact]
        public async Task should_sum_all_values()
        {
            var sequence = await CreateStream("sequence_1");
            var result = await sequence.RunAsync<Sum>();
            Assert.Equal(55, result.Total);
        }

        [Fact]
        public async Task should_sum_all_values_async()
        {
            var sequence = await CreateStream("sequence_1");
            var result = await sequence.RunAsync<SumAsync>();
            Assert.Equal(55, result.Total);
        }


        [Fact]
        public async Task should_sum_only_values_with_odd_index_in_stream()
        {
            var sequence = await CreateStream("sequence_1");
            var result = await sequence.RunWhereAsync<Sum>(c => c.Index % 2 == 1);
            Assert.Equal(25, result.Total);
        }

        [Fact]
        public async Task should_sum_last_two_values()
        {
            var sequence = await CreateStream("sequence_1");
            var result = await sequence.RunAsync<Sum>(fromIndexInclusive: 9);
            Assert.Equal(19, result.Total);
        }

        [Fact(Skip = "Incomplete")]
        public async Task should_snapshot_values()
        {
            var sequence = await CreateStream("sequence_1");
            var result = await sequence.RunAsync<Sum>();
            Assert.Equal(55, result.Total);
        }
    }
}
