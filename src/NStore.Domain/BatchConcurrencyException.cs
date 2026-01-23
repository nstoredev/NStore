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
    [Serializable]
    public class AggregateFailureInfo : ISerializable
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

        // Serialization support
        protected AggregateFailureInfo(SerializationInfo info, StreamingContext context)
        {
            AggregateId = info.GetString(nameof(AggregateId));
            AggregateType = (Type)info.GetValue(nameof(AggregateType), typeof(Type));
            Reason = (AggregateFailureReason)info.GetValue(nameof(Reason), typeof(AggregateFailureReason));
            InnerException = (Exception)info.GetValue(nameof(InnerException), typeof(Exception));
        }

        public virtual void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(nameof(AggregateId), AggregateId);
            info.AddValue(nameof(AggregateType), AggregateType, typeof(Type));
            info.AddValue(nameof(Reason), Reason, typeof(AggregateFailureReason));
            info.AddValue(nameof(InnerException), InnerException, typeof(Exception));
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
}
