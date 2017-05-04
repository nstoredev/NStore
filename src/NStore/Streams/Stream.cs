using System;
using System.Threading.Tasks;
using NStore.Raw.Contracts;

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

        public Task Read(int index, Func<long, object, ScanCallbackResult> consumer)
        {
            return _raw.ScanAsync(this.Id, index, ScanDirection.Forward, consumer);
        }

        public Task Delete()
        {
            return _raw.DeleteAsync(this.Id);
        }
    }
}