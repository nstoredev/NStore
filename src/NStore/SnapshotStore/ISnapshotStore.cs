using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.SnapshotStore
{
    public interface ISnapshotStore
    {
        Task<SnapshotInfo> Get(
            string aggregateId,
            long version,
            CancellationToken cancellationToken
        );

        Task Add(
            string aggregateId,
            SnapshotInfo snapshot,
            CancellationToken cancellationToken
        );

        Task Remove(
            string aggregateId,
            long fromVersionInclusive,
            long toVersionInclusive,
            CancellationToken cancellationToken
        );
    }

    public static class SnapshotStoreExtensions
    {
        public static Task<SnapshotInfo> Get(
            this ISnapshotStore snapshots,
            string aggregateId,
            long version
        )
        {
            return snapshots.Get(aggregateId, version, CancellationToken.None);
        }

        public static Task Add(
            this ISnapshotStore snapshots,
            string aggregateId,
            SnapshotInfo snapshot
        )
        {
            return snapshots.Add(aggregateId, snapshot, CancellationToken.None);
        }

        public static Task Remove(
            this ISnapshotStore snapshots,
            string aggregateId
        )
        {
            return snapshots.Remove(aggregateId, 0, long.MaxValue, CancellationToken.None);
        }

        public static Task Remove(
            this ISnapshotStore snapshots,
            string aggregateId,
            long fromVersionInclusive,
            long toVersionInclusive
        )
        {
            return snapshots.Remove(aggregateId, fromVersionInclusive, toVersionInclusive, CancellationToken.None);
        }
    }
}