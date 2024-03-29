﻿using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public interface IGlobalPersistence
    {
        Task ReadAllAsync(
            long fromPositionInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken
        );

        Task<long> ReadLastPositionAsync(
            CancellationToken cancellationToken
        );

        Task ReadAllByOperationIdAsync(
            string operationId,
            ISubscription subscription,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Replace the chunk at a given position
        /// </summary>
        /// <param name="position">The position to rewrite</param>
        /// <param name="partitionId">New Partition Id</param>
        /// <param name="index">New Index</param>
        /// <param name="payload">New Payload</param>
        /// <param name="operationId">New Opeartion Id</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns></returns>
        Task<IChunk> ReplaceOneAsync(
            long position,
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Read a single chunk given the global position
        /// </summary>
        /// <param name="position">Global position</param>
        /// <param name="cancellationToken"></param>
        /// <returns>Chunk at Global Position</returns>
        Task<IChunk> ReadOneAsync(
            long position,
            CancellationToken cancellationToken
        );
    }
}