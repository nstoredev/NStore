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
        [Fact]
        public void should_overlap()
        {
            var a = new DateRange(DateTime.Parse("2017-01-01"), DateTime.Parse("2017-01-31"));
            var b = new DateRange(DateTime.Parse("2017-01-02"), DateTime.Parse("2017-01-03"));

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
