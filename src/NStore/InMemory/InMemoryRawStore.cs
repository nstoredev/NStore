using System;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.InMemory
{
    public class InMemoryRawStore : IRawStore
    {
        public Task ScanAsync(string partitionId, long indexStart, ScanDirection direction, Func<long, object, ScanCallbackResult> consume, int limit = Int32.MaxValue)
        {
            throw new NotImplementedException();
        }

        public Task ScanStoreAsync(long sequenceStart, ScanDirection direction, Func<long, object, ScanCallbackResult> consume, int limit = Int32.MaxValue)
        {
            throw new NotImplementedException();
        }

        public Task PersistAsync(string partitionId, long index, object payload, string operationId = null)
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync(string partitionId, long fromIndex = 0, long toIndex = Int64.MaxValue)
        {
            throw new NotImplementedException();
        }
    }
}