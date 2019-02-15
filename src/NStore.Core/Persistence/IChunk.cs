namespace NStore.Core.Persistence
{
    /// <summary>
    /// Chunk is the atomic element of the store.
    /// The eventstore is a global ordered sequence of chunks
    /// </summary>
    public interface IChunk
    {
        /// <summary>
        /// Global position
        /// </summary>
        long Position { get; }

        /// <summary>
        /// Partion / stream / aggregate id
        /// </summary>
        string PartitionId { get; }

        /// <summary>
        /// Chunk index for a given partition
        /// </summary>
        long Index { get; }

        /// <summary>
        /// Data
        /// </summary>
        object Payload { get; }

        /// <summary>
        /// Idempotency Key, unique for a given partition / stream / aggregate
        /// </summary>
        string OperationId { get; }
    }
}