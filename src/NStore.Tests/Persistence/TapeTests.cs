using System;
using System.Collections.Generic;
using NStore.Raw;
using Xunit;

namespace NStore.Tests.Persistence
{
    public class TapeTests
    {
        [Fact]
        public void new_tape_is_empty()
        {
            var tape = new PartitionRecorder();

            Assert.True(tape.IsEmpty);
        }

        [Fact]
        public void record()
        {
            var tape = new PartitionRecorder();

            tape.Consume(1, "a");

            Assert.False(tape.IsEmpty);
            Assert.Equal(1, tape.Length);
            Assert.Equal("a", tape[0]);
        }

        [Fact]
        public void replay()
        {
            var tape = new PartitionRecorder();

            tape.Consume(1, "a");

            var list = new List<object>();
            tape.Replay(list.Add);

            Assert.Equal(1, list.Count);
            Assert.Equal("a", list[0]);
        }
    }
}