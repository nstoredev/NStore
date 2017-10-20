using System;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace NStore.Core.Streams
{
    [Serializable]
    public class AppendFailedException : Exception
    {
        public string StreamId { get; }

        public AppendFailedException(string streamId, string message) : base(message)
        {
            StreamId = streamId;
        }

        public AppendFailedException() : base()
        {
        }

        protected AppendFailedException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context)
        {
            this.StreamId = info.GetString("StreamId");
        }

        public AppendFailedException(string message) : base(message)
        {
        }

        public AppendFailedException(string message, Exception innerException) : base(message, innerException)
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

            // MUST call through to the base class to let it save its own state
            base.GetObjectData(info, context);
        }
    }
}