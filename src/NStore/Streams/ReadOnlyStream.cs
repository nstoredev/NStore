using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public class ReadOnlyStream : Stream
    {
        public ReadOnlyStream(string streamId, IRawStore raw) : base(streamId, raw)
        {
        }

        public override Task Append(object payload, string operationId, CancellationToken cancellation = new CancellationToken())
        {
            throw new StreamReadOnlyException();
        }

        public override Task Delete(CancellationToken cancellation = new CancellationToken())
        {
            throw new StreamReadOnlyException();
        }
    }
}