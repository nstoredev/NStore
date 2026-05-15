using System.Collections.Generic;

namespace NStore.Core.Persistence
{
    /// <summary>
    /// Synchronous read-only operations on partitions, for use in contexts
    /// where async is not available.
    /// </summary>
    public interface IPartitionPersistenceSync
    {
        IReadOnlyList<IChunk> ReadForward(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            int limit
        );

        IReadOnlyList<IChunk> ReadBackward(
            string partitionId,
            long fromUpperIndexInclusive,
            long toLowerIndexInclusive,
            int limit
        );

        IChunk ReadSingleBackward(
            string partitionId,
            long fromUpperIndexInclusive
        );

        IChunk ReadByOperationId(
            string partitionId,
            string operationId
        );
    }
}
