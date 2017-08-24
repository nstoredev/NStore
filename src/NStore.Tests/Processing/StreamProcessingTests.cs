using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NStore.InMemory;
using NStore.Processing;
using NStore.Streams;
using Xunit;

namespace NStore.Tests.Processing
{
    public class Sum : PayloadProcessor
    {
        public int Total { get; private set; }

        private void On(SampleData data)
        {
            this.Total += data.Value;
        }
    }

    public class SampleData
    {
        public SampleData(int value)
        {
            Value = value;
        }

        public int Value { get; private set; }
    }

    public class StreamProcessingTests
    {
        readonly StreamsFactory _streams = new StreamsFactory(new InMemoryPersistence());
        private async Task<IStream> CreateStream(string streamId)
        {
            var counter1 = _streams.Open(streamId);
            for (int c = 1; c <= 10; c++)
            {
                await counter1.AppendAsync(new SampleData(c));
            }
            return counter1;
        }

        [Fact]
        public async Task should_sum_all_values()
        {
            var counter1 = await CreateStream("counter_1");

            var sum = new StreamProcessor<Sum>();
            await sum.RunAsync(counter1);

            Assert.Equal(55, sum.Result.Total);
        }

        [Fact]
        public async Task should_sum_only_values_with_odd_index_in_stream()
        {
            var counter1 = await CreateStream("counter_1");

            var sum = new StreamProcessor<Sum>(c => c.Index % 2 == 1);
            await sum.RunAsync(counter1);

            Assert.Equal(25, sum.Result.Total);
        }

        [Fact]
        public async Task should_sum_last_two_values()
        {
            var counter1 = await CreateStream("counter_1");

            var sum = new StreamProcessor<Sum>();
            await sum.RunAsync(counter1, 9);

            Assert.Equal(19, sum.Result.Total);
        }
    }
}
