using System;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace NStore.Core.Persistence
{
    /// <summary>
    /// This exception should be thrown when we are trying to persist a chunk for a
    /// given PartitionId but the Index already exists. This is usually due to concurrency
    /// on the same PartitionId
    /// </summary>
    [Serializable]
    public class DuplicateStreamIndexException : Exception
    {
        public long StreamIndex { get; }
        public string StreamId { get; }

        public DuplicateStreamIndexException(string streamId, long streamIndex) : 
            base($"Duplicated index {streamIndex} on stream {streamId}")
        {
            this.StreamIndex = streamIndex;
            this.StreamId = streamId;
        }

        public DuplicateStreamIndexException()
        {
        }

        protected DuplicateStreamIndexException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.StreamId = info.GetString("StreamId");
            this.StreamIndex = info.GetInt64("StreamIndex");
        }

        public DuplicateStreamIndexException(string message) : base(message)
        {
        }

        public DuplicateStreamIndexException(string message, Exception innerException) : base(message, innerException)
        {
        }

        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException(nameof(info));
            }

            info.AddValue("StreamId", this.StreamId);
            info.AddValue("StreamIndex", this.StreamIndex);

            // MUST call through to the base class to let it save its own state
            base.GetObjectData(info, context);
        }
    }
}