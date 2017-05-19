using NStore.InMemory;
using NStore.Raw;
using Xunit;

namespace NStore.Tests.Persistence
{
    public class StatsDecoratorTests
    {
        private readonly StatsDecorator _stats;
        private readonly IRawStore _store;
        
        public StatsDecoratorTests()
        {
            var nullStore = new NullStore();
            _store = _stats = new StatsDecorator(nullStore);
        }

        [Fact]
        public void counters_should_be_zero()
        {
            Assert.Equal(0, _stats.TotalPersists);
			Assert.Equal(0, _stats.TotalDeletes);
			Assert.Equal(0, _stats.ScanStoreCalls);
			Assert.Equal(0, _stats.ScanPartitionCalls);
		}

        [Fact]
        public async void persist_should_be_recorded()
        {
            await _store.PersistAsync("empty", 0, null);
			Assert.Equal(1, _stats.TotalPersists);
			Assert.Equal(0, _stats.TotalDeletes);
			Assert.Equal(0, _stats.ScanStoreCalls);
			Assert.Equal(0, _stats.ScanPartitionCalls);
        }

        [Fact]
        public async void delete_should_be_recorded()
        {
            await _store.DeleteAsync("empty");
			Assert.Equal(0, _stats.TotalPersists);
			Assert.Equal(1, _stats.TotalDeletes);
			Assert.Equal(0, _stats.ScanStoreCalls);
			Assert.Equal(0, _stats.ScanPartitionCalls);
        }

		[Fact]
		public async void scan_store_should_be_recorded()
		{
            await _store.ScanStoreAsync(0, ScanDirection.Forward, new SuperTape(), 10);
			Assert.Equal(0, _stats.TotalPersists);
			Assert.Equal(0, _stats.TotalDeletes);
			Assert.Equal(1, _stats.ScanStoreCalls);
			Assert.Equal(0, _stats.ScanPartitionCalls);
		}

		[Fact]
		public async void scan_partition_should_be_recorded()
		{
            await _store.ScanPartitionAsync("empty",0, ScanDirection.Forward, new Tape(), 10);
			Assert.Equal(0, _stats.TotalPersists);
			Assert.Equal(0, _stats.TotalDeletes);
			Assert.Equal(0, _stats.ScanStoreCalls);
			Assert.Equal(1, _stats.ScanPartitionCalls);
		}
    }
}