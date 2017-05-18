using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.SnapshotStore
{
    public class NullSnapshots : ISnapshotStore
    {
        public Task<SnapshotInfo> Get(string aggregateId, int version, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.FromResult((SnapshotInfo)null);
        }

        public Task Add(string aggregateId, SnapshotInfo snapshot, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.FromResult(0);
        }

        public Task Remove(
            string aggregateId, 
            int fromVersionInclusive = 0, 
            int toVersionInclusive = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            return Task.FromResult(0);
        }
    }
}