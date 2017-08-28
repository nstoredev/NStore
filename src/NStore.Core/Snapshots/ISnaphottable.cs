namespace NStore.Core.Snapshots
{
    public interface ISnaphottable
    {
        SnapshotInfo GetSnapshot();
        bool TryRestore(SnapshotInfo snapshotInfo);
    }
}