using System.Threading.Tasks;

namespace NStore.SnapshotStore
{
    public sealed class SnapshotInfo
    {
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
        Task<SnapshotInfo> Get(string id, int version);
        Task Add(string aggregateId, SnapshotInfo snapshot);
    }

    public class NullSnapshots : ISnapshotStore
    {
        private readonly SnapshotInfo _nullSnapshot = new SnapshotInfo(0, null);

        public Task<SnapshotInfo> Get(string id, int version)
        {
            return Task.FromResult(_nullSnapshot);
        }

        public Task Add(string aggregateId, SnapshotInfo snapshot)
        {
            return Task.FromResult(0);
        }
    }
}