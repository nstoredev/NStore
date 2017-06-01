using System;
using System.Collections.Generic;
using NStore.Persistence;
using Xunit;

namespace NStore.Tests.Persistence
{
    public class RecorderTests
    {
        public class Data : IPartitionData
        {
            public string PartitionId { get; set; }
            public long Index { get; set; }
            public object Payload { get; set; }
        }

        [Fact]
        public void new_tape_is_empty()
        {
            var tape = new PartitionRecorder();

            Assert.True(tape.IsEmpty);
        }

        [Fact]
        public void record()
        {
            var recorder = new PartitionRecorder();

            recorder.OnNext(new Data{ Index = 1, Payload = "a"});

            Assert.False(recorder.IsEmpty);
            Assert.Equal(1, recorder.Length);
            Assert.Equal("a", recorder[0]);
        }

        [Fact]
        public void replay()
        {
            var recorder = new PartitionRecorder();

            recorder.OnNext(new Data { Index = 1, Payload = "a" });

            var list = new List<object>();
            recorder.Replay(list.Add);

            Assert.Equal(1, list.Count);
            Assert.Equal("a", list[0]);
        }
    }
}