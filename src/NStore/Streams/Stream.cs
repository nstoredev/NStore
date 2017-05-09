using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public abstract class StreamBase
    {
        protected IRawStore Raw { get; }
        public string Id { get; }

        protected StreamBase(string streamId, IRawStore raw)
        {
            this.Id = streamId;
            this.Raw = raw;
        }

        public Task Read(
            int fromIndexInclusive,
            int toIndexInclusive,
            Func<long, object, ScanCallbackResult> consumer,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            return Raw.ScanPartitionAsync(
                this.Id,
                fromIndexInclusive,
                ScanDirection.Forward,
                consumer,
                toIndexInclusive,
                cancellationToken
            );
        }
    }

    public class Stream : StreamBase, IStream
    {
        public Stream(string streamId, IRawStore raw) : base(streamId, raw)
        {
        }

        public Task Append(object payload, string operationId)
        {
            return Raw.PersistAsync(this.Id, -1, payload, operationId);
        }

        public Task Delete()
        {
            return Raw.DeleteAsync(this.Id);
        }
    }

    public class OptimisticConcurrencyStream: StreamBase, IOptimisticConcurrencyStream
    {
        public OptimisticConcurrencyStream(string streamId, IRawStore raw) : base(streamId, raw)
        {
        }

        public Task Append(long version, object payload, string operationId)
        {
            return Raw.PersistAsync(this.Id, version, payload, operationId);
        }

        public Task Delete()
        {
            return Raw.DeleteAsync(this.Id);
        }
    }

}