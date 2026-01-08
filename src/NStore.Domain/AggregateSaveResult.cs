using System;
using NStore.Core.Persistence;

namespace NStore.Domain
{
    /// <summary>
    /// Represents the outcome of attempting to save a single aggregate as part of a batch operation.
    /// The instance contains outcome metadata such as whether the save succeeded, any exception
    /// raised by the persistence layer, the persisted chunk when applicable and an optional
    /// categorized failure kind for programmatic handling by callers.
    /// </summary>
    public class AggregateSaveResult
    {
        /// <summary>
        /// The aggregate identifier for which this save result applies.
        /// </summary>
        public string AggregateId { get; internal set; }

        /// <summary>
        /// True when the save operation completed successfully (or the operation was idempotently skipped).
        /// When false, <see cref="FailureException"/> should contain the cause of the failure.
        /// </summary>
        public bool Succeeded { get; internal set; }

        /// <summary>
        /// When the save operation resulted in a persisted chunk, this property contains that chunk.
        /// Otherwise it is <c>null</c>.
        /// </summary>
        public IChunk Chunk { get; internal set; }

        /// <summary>
        /// A machine-readable, categorized reason for the save outcome. It is <c>null</c> when the
        /// save succeeded and no special condition applies.
        /// </summary>
        public AggregateSaveFailureKind? FailureKind { get; internal set; }
    }
}