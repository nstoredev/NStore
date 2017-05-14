using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public class Stream : IStream
    {
        protected IRawStore Raw { get; }
        public string Id { get; }

        public Stream(string streamId, IRawStore raw)
        {
            this.Id = streamId;
            this.Raw = raw;
        }

        public Task Read(IPartitionObserver partitionObserver, int fromIndexInclusive, int toIndexInclusive, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Raw.ScanPartitionAsync(
                Id,
                fromIndexInclusive,
                ScanDirection.Forward,
                partitionObserver,
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