using System;
using System.Diagnostics;
using System.Threading;
using NStore.Tests.AggregatesTests;
using Xunit;

namespace NStore.Tests
{
    public class TimespanTests
    {
        [Fact]
        public void stopwatch_ticks_are_wrong_on_netcore_macos()
        {
            var sw = new Stopwatch();
            sw.Start();
            Thread.Sleep(100);
            sw.Stop();
            
            var msFromTicks = sw.ElapsedTicks / TimeSpan.TicksPerMillisecond;
            var ms = sw.ElapsedMilliseconds;
            
            Assert.Equal(ms, msFromTicks);
        }
    }
}