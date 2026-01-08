using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using NStore.Core.Streams;

namespace NStore.Domain
{
    /// <summary>
    /// Represents detailed information about a failed aggregate save operation in a batch.
    /// </summary>
    public class AggregateFailureInfo
    {
        /// <summary>
        /// The aggregate ID that failed to save
        /// </summary>
        public string AggregateId { get; }

        /// <summary>
        /// The type of aggregate that failed
        /// </summary>
        public Type AggregateType { get; }

        /// <summary>
        /// The reason for failure
        /// </summary>
        public AggregateFailureReason Reason { get; }

        /// <summary>
        /// The inner exception if available
        /// </summary>
        public Exception InnerException { get; }

        public AggregateFailureInfo(string aggregateId, Type aggregateType, AggregateFailureReason reason, Exception innerException = null)
        {
            AggregateId = aggregateId;
            AggregateType = aggregateType;
            Reason = reason;
            InnerException = innerException;
        }

        public override string ToString()
        {
            return $"{AggregateType.Name}[{AggregateId}]: {Reason}";
        }
    }

    /// <summary>
    /// Reason why an aggregate failed to save in a batch operation
    /// </summary>
    public enum AggregateFailureReason
    {
        /// <summary>
        /// Optimistic concurrency violation - another process modified the aggregate
        /// </summary>
        ConcurrencyConflict,

        /// <summary>
        /// Generic failure not covered by other reasons
        /// </summary>
        Failed
    }

    /// <summary>
    /// Exception thrown when a batch save operation fails due to concurrency conflicts or other errors.
    /// Contains detailed information about which aggregates failed and why.
    /// </summary>
    [Serializable]
    public class BatchConcurrencyException : ConcurrencyException
    {
        /// <summary>
        /// Collection of aggregates that failed to save
        /// </summary>
        public IReadOnlyList<AggregateFailureInfo> FailedAggregates { get; }

        /// <summary>
        /// Collection of aggregate IDs that were saved successfully
        /// </summary>
        public IReadOnlyList<string> SucceededAggregateIds { get; }

        public BatchConcurrencyException(
            IEnumerable<AggregateFailureInfo> failedAggregates,
            IEnumerable<string> succeededAggregateIds)
            : base(BuildMessage(failedAggregates))
        {
            FailedAggregates = failedAggregates?.ToList() ?? new List<AggregateFailureInfo>();
            SucceededAggregateIds = succeededAggregateIds?.ToList() ?? new List<string>();
        }

        public BatchConcurrencyException(
            IEnumerable<AggregateFailureInfo> failedAggregates,
            IEnumerable<string> succeededAggregateIds,
            Exception innerException)
            : base(BuildMessage(failedAggregates), innerException)
        {
            FailedAggregates = failedAggregates?.ToList() ?? new List<AggregateFailureInfo>();
            SucceededAggregateIds = succeededAggregateIds?.ToList() ?? new List<string>();
        }

        private static string BuildMessage(IEnumerable<AggregateFailureInfo> failedAggregates)
        {
            var failures = failedAggregates?.ToList() ?? new List<AggregateFailureInfo>();
            if (!failures.Any())
            {
                return "Batch save operation failed";
            }

            var concurrencyFailures = failures.Where(f => f.Reason == AggregateFailureReason.ConcurrencyConflict).ToList();
            var otherFailures = failures.Where(f => f.Reason != AggregateFailureReason.ConcurrencyConflict).ToList();

            var messageParts = new List<string>();

            if (concurrencyFailures.Any())
            {
                messageParts.Add($"Concurrency conflicts on {concurrencyFailures.Count} aggregate(s): {string.Join(", ", concurrencyFailures.Select(f => f.AggregateId))}");
            }

            if (otherFailures.Any())
            {
                messageParts.Add($"Other failures on {otherFailures.Count} aggregate(s): {string.Join(", ", otherFailures.Select(f => $"{f.AggregateId}({f.Reason})"))}");
            }

            return string.Join("; ", messageParts);
        }

        /// <summary>
        /// Gets all aggregates that failed due to concurrency conflicts
        /// </summary>
        public IEnumerable<AggregateFailureInfo> GetConcurrencyConflicts()
        {
            return FailedAggregates.Where(f => f.Reason == AggregateFailureReason.ConcurrencyConflict);
        }

        /// <summary>
        /// Checks if there are any concurrency conflicts
        /// </summary>
        public bool HasConcurrencyConflicts => FailedAggregates.Any(f => f.Reason == AggregateFailureReason.ConcurrencyConflict);
    }
}
