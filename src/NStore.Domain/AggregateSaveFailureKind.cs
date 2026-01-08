using System;

namespace NStore.Domain
{
    /// <summary>
    /// Represents the categorized reason for an individual aggregate save outcome.
    /// </summary>
    public enum AggregateSaveFailureKind
    {
        /// <summary>
        /// Optimistic concurrency conflict (another process updated the aggregate index).
        /// </summary>
        Concurrency,

        /// <summary>
        /// Generic / unknown failure reported by the persistence layer.
        /// </summary>
        GenericFailure,

        /// <summary>
        /// Position conflict after retry limit was exceeded when attempting to append.
        /// </summary>
        DuplicatedPosition,

        /// <summary>
        /// Operation was already executed (idempotent skip). Note: in this case the save is considered succeeded
        /// but the operation did not produce a new change (or chunk may be present depending on persistence).
        /// </summary>
        DuplicatedOperation,

        /// <summary>
        /// The aggregate had no changes to persist. The operation is considered successful but no chunk
        /// was written for this aggregate.
        /// </summary>
        Unchanged,

        /// <summary>
        /// The aggregate failed its invariants check when attempting to save. The save is considered failed
        /// and the corresponding <see cref="AggregateSaveResult.FailureException"/> will contain an
        /// <see cref="InvariantCheckFailedException"/>.
        /// </summary>
        InvariantFailure
    }
}
