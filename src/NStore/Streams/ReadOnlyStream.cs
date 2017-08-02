using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;

namespace NStore.Streams
{
    public class ReadOnlyStream : Stream
    {
        public override bool IsWritable => false;

        public ReadOnlyStream(string streamId, IPersistence persistence) : base(streamId, persistence)
        {
        }

        public override Task AppendAsync(object payload, string operationId, CancellationToken cancellation )
        {
            throw new StreamReadOnlyException();
        }

        public override Task DeleteAsync(CancellationToken cancellation )
        {
            throw new StreamReadOnlyException();
        }
    }
}