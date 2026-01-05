using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Domain
{
    /// <summary>
    /// Batch repository for efficient multi-aggregate operations.
    /// Enables loading and saving multiple aggregates in a single batch operation.
    /// </summary>
    public interface IBatchRepository
    {
        /// <summary>
        /// Loads multiple aggregates of the same type by their IDs.
        /// Uses batch snapshot reading and multi-partition event reading for optimal performance.
        /// </summary>
        /// <typeparam name="T">The aggregate type</typeparam>
        /// <param name="ids">Collection of aggregate IDs to load</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Dictionary mapping aggregate IDs to loaded aggregates. Only successfully loaded aggregates are included.</returns>
        Task<IDictionary<string, T>> GetManyByIdAsync<T>(
            IEnumerable<string> ids,
            CancellationToken cancellationToken = default
        ) where T : IAggregate;

        /// <summary>
        /// Saves multiple aggregates in a single batch operation and returns a `BatchSaveResult` describing per-aggregate outcomes.
        /// Throws BatchConcurrencyException if any aggregate has a concurrency conflict (per-aggregate result population will be implemented later).
        /// Automatically retries on position conflicts (duplicate global IDs).
        /// </summary>
        /// <param name="aggregates">Collection of aggregates to save</param>
        /// <param name="operationId">Operation ID for idempotency, it can be null, where using null means
        /// that the repository will generate unique operation id. Usually I'm expecting this value to be pased 
        /// from the external code, because probably all the update of all the aggregate will be written on the 
        /// very same operation id for a full idempotency: es the caller calls an api, a transport error arise, he/she can
        /// request the same batch of operation with the same id for full idempotency.</param>
        /// <param name="headers">Optional action to add headers to changesets</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <exception cref="BatchConcurrencyException">Thrown when one or more aggregates have concurrency conflicts</exception>
        Task<BatchSaveResult> SaveManyAsync(
            IEnumerable<IAggregate> aggregates,
            string operationId,
            Action<IHeadersAccessor> headers = null,
            CancellationToken cancellationToken = default
        );

        /// <summary>
        /// Clears all internal tracking state.
        /// Useful after concurrency exceptions to reload aggregates for retry.
        /// </summary>
        void Clear();
    }
}
