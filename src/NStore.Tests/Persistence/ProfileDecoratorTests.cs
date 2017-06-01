using System.Threading;
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

        [Fact]
        public void counters_should_be_zero()
        {
            Assert.Equal(0, _profile.PersistCounter.Calls);
			Assert.Equal(0, _profile.DeleteCounter.Calls);
			Assert.Equal(0, _profile.StoreScanCounter.Calls);
			Assert.Equal(0, _profile.PartitionReadForwardCounter.Calls);
			Assert.Equal(0, _profile.PartitionReadBackwardCounter.Calls);
			Assert.Equal(0, _profile.PeekCounter.Calls);
		}

        [Fact]
        public async void persist_should_be_recorded()
        {
            await _store.PersistAsync("empty", 0, null);
            Assert.Equal(1, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
			Assert.Equal(0, _profile.StoreScanCounter.Calls);
			Assert.Equal(0, _profile.PartitionReadForwardCounter.Calls);
            Assert.Equal(0, _profile.PartitionReadBackwardCounter.Calls);
            Assert.Equal(0, _profile.PeekCounter.Calls);
        }

        [Fact]
        public async void delete_should_be_recorded()
        {
            await _store.DeleteAsync("empty");
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(1, _profile.DeleteCounter.Calls);
			Assert.Equal(0, _profile.StoreScanCounter.Calls);
			Assert.Equal(0, _profile.PartitionReadForwardCounter.Calls);
            Assert.Equal(0, _profile.PartitionReadBackwardCounter.Calls);
            Assert.Equal(0, _profile.PeekCounter.Calls);
        }

        [Fact]
		public async void scan_store_should_be_recorded()
		{
            await _store.ScanStoreAsync(0, ReadDirection.Forward, new AllPartitionsRecorder(), 10);
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
			Assert.Equal(1, _profile.StoreScanCounter.Calls);
			Assert.Equal(0, _profile.PartitionReadForwardCounter.Calls);
		    Assert.Equal(0, _profile.PartitionReadBackwardCounter.Calls);
		    Assert.Equal(0, _profile.PeekCounter.Calls);
		}

        [Fact]
		public async void scan_partition_should_be_recorded()
		{
            await _store.ReadPartitionForward("empty",0, new PartitionRecorder(), 10);
		    Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
			Assert.Equal(0, _profile.StoreScanCounter.Calls);
			Assert.Equal(1, _profile.PartitionReadForwardCounter.Calls);
		    Assert.Equal(0, _profile.PartitionReadBackwardCounter.Calls);
		    Assert.Equal(0, _profile.PeekCounter.Calls);
		}

        [Fact]
        public async void scan_partition_backward_should_be_recorded()
        {
            await _store.ReadPartitionBackward("empty", 0, new PartitionRecorder(), 10);
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
            Assert.Equal(0, _profile.StoreScanCounter.Calls);
            Assert.Equal(0, _profile.PartitionReadForwardCounter.Calls);
            Assert.Equal(1, _profile.PartitionReadBackwardCounter.Calls);
            Assert.Equal(0, _profile.PeekCounter.Calls);
        }

        [Fact]
        public async void peek_partition_should_be_recorded()
        {
            var value = await _store.PeekPartition("empty", 1, CancellationToken.None);
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
            Assert.Equal(0, _profile.StoreScanCounter.Calls);
            Assert.Equal(0, _profile.PartitionReadForwardCounter.Calls);
            Assert.Equal(0, _profile.PartitionReadBackwardCounter.Calls);
            Assert.Equal(1, _profile.PeekCounter.Calls);
        }
    }
}