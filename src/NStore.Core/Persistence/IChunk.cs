namespace NStore.Core.Persistence
{
    public interface IChunk
    {
        long Position { get; }
        string PartitionId { get; }
        long Index { get; }
        object Payload { get; }
        string OperationId { get; }
    }
}