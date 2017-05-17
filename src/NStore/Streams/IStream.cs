using System.Threading;
using System.Threading.Tasks;

namespace NStore.Streams
{
    public interface IStream : IReadOnlyStream
    {
        //@@TODO add New() to avoid a db rountrip?

        bool IsWritable { get; }

        /// <summary>
        /// Append
        /// </summary>
        /// <param name="payload"></param>
        /// <param name="operationId"></param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        Task Append(object payload, string operationId = null, CancellationToken cancellation = default(CancellationToken));

        /// <summary>
        /// Delete
        /// </summary>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        Task Delete(CancellationToken cancellation = default(CancellationToken));
    }
}