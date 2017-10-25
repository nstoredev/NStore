using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Core.Streams
{
    public class ReadOnlyStream : Stream
    {
        public override bool IsWritable => false;

        public ReadOnlyStream(string streamId, IPersistence persistence) : base(streamId, persistence)
        {
        }

        public override Task<IChunk> AppendAsync(object payload, string operationId, CancellationToken cancellation )
        {
            throw new StreamReadOnlyException();
        }

        public override Task DeleteAsync(CancellationToken cancellation )
        {
            throw new StreamReadOnlyException();
        }
    }
}