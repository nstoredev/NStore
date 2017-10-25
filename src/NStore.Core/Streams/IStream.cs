using NStore.Core.Persistence;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Streams
{
    public interface IStream : IReadOnlyStream
    {
        //@@REVIEW add New() to avoid a db rountrip?

        bool IsWritable { get; }

        Task<IChunk> AppendAsync(object payload, string operationId, CancellationToken cancellation);

        Task DeleteAsync(CancellationToken cancellation);
    }
}