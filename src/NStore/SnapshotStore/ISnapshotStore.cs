using System.Threading;
using System.Threading.Tasks;

namespace NStore.SnapshotStore
{
    public sealed class SnapshotInfo
    {
        public SnapshotInfo(string aggregateId, int aggregateVersion, object data, int snapshotVersion)
        {
            AggregateVersion = aggregateVersion;
            Data = data;
            SnapshotVersion = snapshotVersion;
            AggregateId = aggregateId;
        }

        public int AggregateVersion { get; private set; }
        public object Data { get; private set; }
        public int SnapshotVersion { get; private set; }
        public string AggregateId { get; private set; }

        public bool IsEmpty => this.AggregateId == null ||
                                this.AggregateVersion == 0 ||
                                this.Data == null ;
    }

    public interface ISnapshotStore
    {
        Task<SnapshotInfo> Get(string id, int version, CancellationToken cancellationToken = default(CancellationToken));
        Task Add(string aggregateId, SnapshotInfo snapshot, CancellationToken cancellationToken = default(CancellationToken));
    }

    public class NullSnapshots : ISnapshotStore
    {
        public Task<SnapshotInfo> Get(string id, int version, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.FromResult((SnapshotInfo)null);
        }

        public Task Add(string aggregateId, SnapshotInfo snapshot, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.FromResult(0);
        }
    }
}