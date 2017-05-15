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
        SnapshotInfo Get(string id, int version);
        bool Updatable { get; }
        Task Add(string aggregateId, SnapshotInfo snapshot);
    }

    public class NullSnapshots : ISnapshotStore
    {
        public SnapshotInfo Get(string id, int version)
        {
            return new SnapshotInfo(0, null);
        }

        public bool Updatable => false;
        public Task Add(string aggregateId, SnapshotInfo snapshot)
        {
            throw new System.NotImplementedException();
        }
    }
}