using System.Threading;
using System.Threading.Tasks;

namespace NStore.SnapshotStore
{
    //@@TODO https://github.com/ProximoSrl/NStore/issues/33
    public sealed class SnapshotInfo
    {
        public static readonly SnapshotInfo Empty = new SnapshotInfo(0, null);

        public SnapshotInfo(int version, object data)
        {
            Version = version;
            Data = data;
        }

        public int Version { get; private set; }
        public object Data { get; private set; }
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
            return Task.FromResult(SnapshotInfo.Empty);
        }

        public Task Add(string aggregateId, SnapshotInfo snapshot, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.FromResult(0);
        }
    }
}