using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.SnapshotStore
{
    public interface ISnapshotStore
    {
        Task<SnapshotInfo> Get(
            string aggregateId, 
            int version, 
            CancellationToken cancellationToken = default(CancellationToken)
        );

        Task Add(
            string aggregateId, 
            SnapshotInfo snapshot, 
            CancellationToken cancellationToken = default(CancellationToken)
        );

        Task Remove(
            string aggregateId,
            int fromVersionInclusive = 0,
            int toVersionInclusive = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        );
    }
}