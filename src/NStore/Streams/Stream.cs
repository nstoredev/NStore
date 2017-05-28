using System;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public class Stream : IStream
    {
        private IRawStore Raw { get; }
        public string Id { get; }
        public virtual bool IsWritable => true;
        public Stream(string streamId, IRawStore raw)
        {
            this.Id = streamId;
            this.Raw = raw;
        }

        public Task Read(IPartitionConsumer partitionConsumer, int fromIndexInclusive, int toIndexInclusive, CancellationToken cancellationToken)
        {
            return Raw.ReadPartitionForward(
                Id,
                fromIndexInclusive,
                partitionConsumer,
                toIndexInclusive,
                int.MaxValue,
                cancellationToken
            );
        }
 
        public virtual Task Append(object payload, string operationId, CancellationToken cancellation)
        {
            return Raw.PersistAsync(this.Id, -1, payload, operationId, cancellation);
        }

        public virtual Task Delete(CancellationToken cancellation)
        {
            return Raw.DeleteAsync(this.Id, cancellationToken: cancellation);
        }
    }
}