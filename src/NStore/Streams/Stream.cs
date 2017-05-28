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

        public Task Read(IPartitionConsumer partitionConsumer)
        {
            return Read(partitionConsumer, 0, int.MaxValue, default(CancellationToken));
        }
        
        public Task Read(IPartitionConsumer partitionConsumer, int fromIndexInclusive)
        {
            return Read(partitionConsumer, fromIndexInclusive, int.MaxValue, default(CancellationToken));
        }

        public Task Read(IPartitionConsumer partitionConsumer, int fromIndexInclusive, CancellationToken cancellationToken)
        {
            return Read(partitionConsumer, fromIndexInclusive, int.MaxValue, cancellationToken);
        }

        public Task Read(IPartitionConsumer partitionConsumer, int fromIndexInclusive, int toIndexInclusive)
        {
            return Read(partitionConsumer, fromIndexInclusive, toIndexInclusive, default(CancellationToken));
        }

        public Task Read(IPartitionConsumer partitionConsumer, int fromIndexInclusive, int toIndexInclusive, CancellationToken cancellationToken)
        {
            return Raw.ReadPartitionForward(
                Id,
                fromIndexInclusive,
                partitionConsumer,
                toIndexInclusive,
                limit:Int32.MaxValue,
                cancellationToken: cancellationToken
            );
        }

        public Task Append(object payload)
        {
            return Append(payload, null, default(CancellationToken));
        }

        public Task Append(object payload, CancellationToken cancellation)
        {
            return Append(payload, null, cancellation);
        }

        public Task Append(object payload, string operationId)
        {
            return Append(payload, operationId, default(CancellationToken));
        }
 
        public virtual Task Append(object payload, string operationId, CancellationToken cancellation)
        {
            return Raw.PersistAsync(this.Id, -1, payload, operationId, cancellation);
        }

        public Task Delete()
        {
            return Delete(default(CancellationToken));
        }

        public virtual Task Delete(CancellationToken cancellation)
        {
            return Raw.DeleteAsync(this.Id, cancellationToken: cancellation);
        }
    }
}