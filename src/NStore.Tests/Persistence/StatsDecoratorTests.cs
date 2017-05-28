using NStore.InMemory;
using NStore.Raw;
using Xunit;

namespace NStore.Tests.Persistence
{
    public class StatsDecoratorTests
    {
        private readonly ProfileDecorator _profile;
        private readonly IRawStore _store;
        
        public StatsDecoratorTests()
        {
            var nullStore = new NullStore();
            _store = _profile = new ProfileDecorator(nullStore);
        }

        [Fact]
        public void counters_should_be_zero()
        {
            Assert.Equal(0, _profile.PersistCounter.Calls);
			Assert.Equal(0, _profile.DeleteCounter.Calls);
			Assert.Equal(0, _profile.StoreScanCounter.Calls);
			Assert.Equal(0, _profile.PartitionReadForwardCounter.Calls);
		}

        [Fact]
        public async void persist_should_be_recorded()
        {
            await _store.PersistAsync("empty", 0, null);
            Assert.Equal(1, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
			Assert.Equal(0, _profile.StoreScanCounter.Calls);
			Assert.Equal(0, _profile.PartitionReadForwardCounter.Calls);
        }

        [Fact]
        public async void delete_should_be_recorded()
        {
            await _store.DeleteAsync("empty");
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(1, _profile.DeleteCounter.Calls);
			Assert.Equal(0, _profile.StoreScanCounter.Calls);
			Assert.Equal(0, _profile.PartitionReadForwardCounter.Calls);
        }

		[Fact]
		public async void scan_store_should_be_recorded()
		{
            await _store.ScanStoreAsync(0, ScanDirection.Forward, new StoreRecorder(), 10);
            Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
			Assert.Equal(1, _profile.StoreScanCounter.Calls);
			Assert.Equal(0, _profile.PartitionReadForwardCounter.Calls);
		}

		[Fact]
		public async void scan_partition_should_be_recorded()
		{
            await _store.ReadPartitionForward("empty",0, new PartitionRecorder(), 10);
		    Assert.Equal(0, _profile.PersistCounter.Calls);
            Assert.Equal(0, _profile.DeleteCounter.Calls);
			Assert.Equal(0, _profile.StoreScanCounter.Calls);
			Assert.Equal(1, _profile.PartitionReadForwardCounter.Calls);
		}
    }
}