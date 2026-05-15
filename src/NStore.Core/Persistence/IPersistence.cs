namespace NStore.Core.Persistence
{
    public interface IPersistence : IPartitionPersistence, IPartitionPersistenceSync, IGlobalPersistence, IMultiPartitionPersistenceReader
    {
    }
}