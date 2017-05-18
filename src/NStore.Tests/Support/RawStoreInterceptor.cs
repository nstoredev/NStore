using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Tests.Support
{
    public class RawStoreInterceptor : IRawStore
    {
        private readonly IRawStore _inner;

        public RawStoreInterceptor(IRawStore inner)
        {
            _inner = inner;
        }

        public void Clear()
        {

        }

        public Task ScanPartitionAsync(
            string partitionId, 
            long fromIndexInclusive, 
            ScanDirection direction,
            IPartitionObserver partitionObserver, 
            long toIndexInclusive = Int64.MaxValue, 
            int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return _inner.ScanPartitionAsync(
                partitionId, 
                fromIndexInclusive, 
                direction, 
                partitionObserver,
                toIndexInclusive, 
                limit, 
                cancellationToken
            );
        }

        public Task ScanStoreAsync(
            long sequenceStart, 
            ScanDirection direction, 
            IStoreObserver observer, 
            int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return _inner.ScanStoreAsync(
                sequenceStart, 
                direction, 
                observer,
                limit,
                cancellationToken
            );
        }

        public Task PersistAsync(
            string partitionId, 
            long index, 
            object payload, 
            string operationId = null,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return _inner.PersistAsync(
                partitionId, 
                index, 
                payload,
                operationId,
                cancellationToken
            );
        }

        public Task DeleteAsync(
            string partitionId, 
            long fromIndex = 0, 
            long toIndex = Int64.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return _inner.DeleteAsync(
                partitionId,
                fromIndex,
                toIndex,
                cancellationToken
            );
        }
    }
}
