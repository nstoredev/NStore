using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.SnapshotStore
{
    public class NullSnapshots : ISnapshotStore
    {
        public Task<SnapshotInfo> GetAsync(string partitionId, long version, CancellationToken cancellationToken)
        {
            return Task.FromResult((SnapshotInfo)null);
        }

        public Task AddAsync(string partitionId, SnapshotInfo snapshot, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string partitionId, long fromVersionInclusive, long toVersionInclusive, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}