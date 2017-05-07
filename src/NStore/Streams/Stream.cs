using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public class Stream : IStream
    {
        private readonly IRawStore _raw;
        public string Id { get;  }

        public Stream(string streamId, IRawStore raw)
        {
            this.Id = streamId;
            _raw = raw;
        }


        public Task Append(string payload, string operationId)
        {
            return _raw.PersistAsync(this.Id, -1, payload, operationId);
        }

        public Task Read(
            int fromIndexInclusive,
            int toIndexInclusive,
            Func<long, object, ScanCallbackResult> consumer,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            return _raw.ScanPartitionAsync(
                this.Id,
                fromIndexInclusive,
                ScanDirection.Forward,
                consumer,
                cancellationToken: cancellationToken
            );
        }

        public Task Delete()
        {
            return _raw.DeleteAsync(this.Id);
        }
    }
}