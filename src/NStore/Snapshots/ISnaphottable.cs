namespace NStore.Snapshots
{
    public interface ISnaphottable
    {
        SnapshotInfo GetSnapshot();
        bool TryRestore(SnapshotInfo snapshotInfo);
    }
}