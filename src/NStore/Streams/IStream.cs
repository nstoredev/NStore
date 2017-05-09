using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public interface IReadOnlyStream
    {
        /// <summary>
        /// Read from stream
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="fromIndexInclusive"></param>
        /// <param name="toIndexInclusive"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task Read(
            IConsumer consumer, 
            int fromIndexInclusive = 0, 
            int toIndexInclusive = Int32.MaxValue, 
            CancellationToken cancellationToken = default(CancellationToken)
        );
    }

    public interface IStream : IReadOnlyStream
    {
        //@@TODO add New() to avoid a db rountrip?

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