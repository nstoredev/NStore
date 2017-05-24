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

        public Task Read(IPartitionConsumer partitionConsumer, int fromIndexInclusive, int toIndexInclusive, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Raw.ScanPartitionAsync(
                Id,
                fromIndexInclusive,
                ScanDirection.Forward,
                partitionConsumer,
                toIndexInclusive,
                cancellationToken: cancellationToken
            );
        }


        public virtual Task Append(object payload, string operationId, CancellationToken cancellation = default(CancellationToken))
        {
            return Raw.PersistAsync(this.Id, -1, payload, operationId, cancellation);
        }

        public virtual Task Delete(CancellationToken cancellation = default(CancellationToken))
        {
            return Raw.DeleteAsync(this.Id, cancellationToken: cancellation);
        }
    }
}