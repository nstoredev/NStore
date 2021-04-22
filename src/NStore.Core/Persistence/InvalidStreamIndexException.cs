using System;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace NStore.Core.Persistence
{
    /// <summary>
    /// This exception should be thrown when we are trying to persist a chunk for a
    /// given PartitionId but the Index is negative.
    /// </summary>
    [Serializable]
    public class InvalidStreamIndexException : Exception
    {
        public long StreamIndex { get; }
        public string StreamId { get; }

        public InvalidStreamIndexException(string streamId, long streamIndex) : 
            base($"Duplicated index {streamIndex} on stream {streamId}")
        {
            this.StreamIndex = streamIndex;
            this.StreamId = streamId;
        }

        public InvalidStreamIndexException()
        {
        }

        protected InvalidStreamIndexException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.StreamId = info.GetString("StreamId");
            this.StreamIndex = info.GetInt64("StreamIndex");
        }

        public InvalidStreamIndexException(string message) : base(message)
        {
        }

        public InvalidStreamIndexException(string message, Exception innerException) : base(message, innerException)
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