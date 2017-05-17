using System;
using System.Collections.Generic;
using System.Text;
using NStore.Sample.Domain.Room;
using Xunit;
// ReSharper disable InconsistentNaming

namespace NStore.Sample.Tests
{
    public class DateRangeTests
    {
        [Theory]
        // same range
        [InlineData("2017-01-01","2017-01-01","2017-01-01","2017-01-01")]
        // b inside a
        [InlineData("2017-01-01","2017-01-31","2017-01-02","2017-01-03")]
        // b start in a
        [InlineData("2017-01-01","2017-01-03","2017-01-02","2017-01-04")]
        // b end in a
        [InlineData("2017-01-10","2017-01-20","2017-01-09","2017-01-11")]
        public void should_overlap(string af, string at, string bf, string bt)
        {
            var a = new DateRange(DateTime.Parse(af), DateTime.Parse(at));
            var b = new DateRange(DateTime.Parse(bf), DateTime.Parse(bt));

            Assert.True(a.Overlaps(b));
            Assert.True(b.Overlaps(a));
        }

        [Fact]
        public void should_not_overlap()
        {
            var a = new DateRange(DateTime.Parse("2017-01-01"), DateTime.Parse("2017-01-02"));
            var b = new DateRange(DateTime.Parse("2017-01-02"), DateTime.Parse("2017-01-03"));

            Assert.False(a.Overlaps(b));
            Assert.False(b.Overlaps(a));
        }
    }
}
