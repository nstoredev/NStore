using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NStore.Persistence;
using Xunit;

namespace NStore.Tests.Persistence
{
    public class RecorderTests
    {
        private class RecorderChunk : IChunk
        {
            public long Position { get; set; }
            public string PartitionId { get; set; }
            public long Index { get; set; }
            public object Payload { get; set; }
            public string OperationId { get; set; }
        }

        [Fact]
        public void new_tape_is_empty()
        {
            var tape = new Recorder();

            Assert.True(tape.IsEmpty);
        }

        [Fact]
        public async Task record()
        {
            var recorder = new Recorder();

            await recorder.OnNextAsync(new RecorderChunk { Index = 1, Payload = "a" });

            Assert.False(recorder.IsEmpty);
            Assert.Equal(1, recorder.Length);
            Assert.Equal("a", recorder[0].Payload);
        }

        [Fact]
        public async Task replay()
        {
            var recorder = new Recorder();
            var chunk = new RecorderChunk {Index = 1, Payload = "a"};

            await recorder.OnNextAsync(chunk);

            var list = new List<IChunk>();
            recorder.Replay(list.Add);

            Assert.True(1 == list.Count);
            Assert.Same(chunk, list[0]);
        }
    }
}