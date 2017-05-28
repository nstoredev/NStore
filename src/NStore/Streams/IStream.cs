using System.Threading;
using System.Threading.Tasks;

namespace NStore.Streams
{
    public interface IStream : IReadOnlyStream
    {
        //@@REVIEW add New() to avoid a db rountrip?

        bool IsWritable { get; }

        Task Append(object payload, string operationId, CancellationToken cancellation);
        Task Delete(CancellationToken cancellation);
    }

    public static class StreamExtensions
    {
        public static Task Append(this IStream stream, object payload)
        {
            return stream.Append(payload, null, CancellationToken.None);
        }

        public static Task Append(this IStream stream, object payload, string operationId)
        {
            return stream.Append(payload, operationId, CancellationToken.None);
        }

        public static Task Delete(this IStream stream)
        {
            return stream.Delete(CancellationToken.None);
        }
    }
}