using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NStore.InMemory;
using NStore.Persistence;
using Xunit;

namespace NStore.Tests.Persistence
{
    public class ProfileDecoratorTests
    {
        private readonly ProfileDecorator _profile;
        private readonly IPersistence _store;

        public ProfileDecoratorTests()
        {
            var nullStore = new NullPersistence();
            _store = _profile = new ProfileDecorator(nullStore);
        }

        private async Task Wait(int ms)
        {
            await Task.Delay(ms);
        }

        [Fact]
        public async void should_count_wait_time()
        {
            var pi = new TaskProfilingInfo("test");
            var sw = new Stopwatch();
            sw.Start();
            await pi.CaptureAsync(() => Wait(1000));
            sw.Stop();

            Assert.True(sw.ElapsedMilliseconds >= 1000, "sw.ElapsedMilliseconds >= 1000");
            Assert.True(pi.Elapsed.TotalMilliseconds >= 1000, "pi.Elapsed.Milliseconds >= 1000");
        }

        [Fact]
        public void counters_should_be_zero()
        {
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
            Assert.Equal(0, _profile.StoreScanCounter.Calls);
            Assert.Equal(0, _profile.ReadForwardCounter.Calls);
            Assert.Equal(0, _profile.ReadBackwardCounter.Calls);
            Assert.Equal(0, _profile.PeekCounter.Calls);
        }

        [Fact]
        public async void persist_should_be_recorded()
        {
            await _store.AppendAsync("empty", 0, null);
            Assert.Equal(1, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
            Assert.Equal(0, _profile.StoreScanCounter.Calls);
            Assert.Equal(0, _profile.ReadForwardCounter.Calls);
            Assert.Equal(0, _profile.ReadBackwardCounter.Calls);
            Assert.Equal(0, _profile.PeekCounter.Calls);
        }

        [Fact]
        public async void delete_should_be_recorded()
        {
            await _store.DeleteAsync("empty");
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(1, _profile.DeleteCounter.Calls);
            Assert.Equal(0, _profile.StoreScanCounter.Calls);
            Assert.Equal(0, _profile.ReadForwardCounter.Calls);
            Assert.Equal(0, _profile.ReadBackwardCounter.Calls);
            Assert.Equal(0, _profile.PeekCounter.Calls);
        }

        [Fact]
        public async void scan_store_should_be_recorded()
        {
            await _store.ReadAllAsync(0, new AllPartitionsRecorder(), 10);
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
            Assert.Equal(1, _profile.StoreScanCounter.Calls);
            Assert.Equal(0, _profile.ReadForwardCounter.Calls);
            Assert.Equal(0, _profile.ReadBackwardCounter.Calls);
            Assert.Equal(0, _profile.PeekCounter.Calls);
        }

        [Fact]
        public async void scan_partition_should_be_recorded()
        {
            await _store.ReadForwardAsync("empty", 0, new Recorder(), 10);
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
            Assert.Equal(0, _profile.StoreScanCounter.Calls);
            Assert.Equal(1, _profile.ReadForwardCounter.Calls);
            Assert.Equal(0, _profile.ReadBackwardCounter.Calls);
            Assert.Equal(0, _profile.PeekCounter.Calls);
        }

        [Fact]
        public async void scan_partition_backward_should_be_recorded()
        {
            await _store.ReadBackwardAsync("empty", 0, new Recorder(), 10);
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
            Assert.Equal(0, _profile.StoreScanCounter.Calls);
            Assert.Equal(0, _profile.ReadForwardCounter.Calls);
            Assert.Equal(1, _profile.ReadBackwardCounter.Calls);
            Assert.Equal(0, _profile.PeekCounter.Calls);
        }

        [Fact]
        public async void peek_partition_should_be_recorded()
        {
            var value = await _store.ReadSingleBackwardAsync("empty", 1, CancellationToken.None);
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
            Assert.Equal(0, _profile.StoreScanCounter.Calls);
            Assert.Equal(0, _profile.ReadForwardCounter.Calls);
            Assert.Equal(0, _profile.ReadBackwardCounter.Calls);
            Assert.Equal(1, _profile.PeekCounter.Calls);
        }
    }
}