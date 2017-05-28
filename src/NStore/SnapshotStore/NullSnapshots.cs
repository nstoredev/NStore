using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.SnapshotStore
{
    public class NullSnapshots : ISnapshotStore
    {
        public Task<SnapshotInfo> Get(
            string aggregateId, 
            int version, 
            CancellationToken cancellationToken)
        {
            return Task.FromResult((SnapshotInfo)null);
        }

        public Task Add(
            string aggregateId, 
            SnapshotInfo snapshot, 
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task Remove(
            string aggregateId, 
            int fromVersionInclusive, 
            int toVersionInclusive,
            CancellationToken cancellationToken 
        )
        {
            return Task.CompletedTask;
        }
    }
}