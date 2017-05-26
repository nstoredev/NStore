using System.Threading;
using System.Threading.Tasks;

namespace NStore.Streams
{
    public interface IStream : IReadOnlyStream
    {
        //@@REVIEW add New() to avoid a db rountrip?

        bool IsWritable { get; }


        Task Append(object payload);
        Task Append(object payload, CancellationToken cancellation);
        Task Append(object payload, string operationId);
        Task Append(object payload, string operationId, CancellationToken cancellation);

        Task Delete();
        Task Delete(CancellationToken cancellation);
    }
}