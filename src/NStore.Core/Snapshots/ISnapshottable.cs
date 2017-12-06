namespace NStore.Core.Snapshots
{
    public interface ISnapshottable
    {
        SnapshotInfo GetSnapshot();
        bool TryRestore(SnapshotInfo snapshotInfo);
    }
}