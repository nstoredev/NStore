using NStore.Core.Persistence;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Streams
{
    public record struct BatchStreamReadRequest(string StreamId, long FromIndexInclusive);

    public record struct BatchAppendRequest(string StreamId, object Payload, string OperationId);

    public record struct BatchStreamAppendResult(bool Success);

    /// <summary>
    /// We have the need to minimize the call to the database, especially for remote 
    /// databases to minimize latency. We have <see cref="IEnhancedPersistence"/>
    /// that is capable of appending batches of events in one call, so we need to maintain
    /// the concept of stream but that is capable of working in batch handling multiple
    /// streams at once if needed.
    /// </summary>
    public interface IBatchStream
    {
        /// <summary>
        /// Batched streams works differently from a standard stream
        /// where usually you specify the stream id in the constructor
        /// then you have method to work with that stream.
        /// 
        /// A batched stream has a specific purpose of loading multiple
        /// stream and being able to batch updates, no other functionalities
        /// are needed
        /// </summary>
        /// <param name="subscription"></param>
        /// <param name="requests"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task ReadBatchAsync(
            ISubscription subscription,
            IEnumerable<BatchStreamReadRequest> requests,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Append a series of payload to multiple streams.
        /// 
        /// It is mandatory that you previously read the
        /// streams you want to append to using <see cref="ReadBatchAsync"/>
        /// this is because the only implementation in the first
        /// step is the concurrency batch async, where we need to 
        /// load the stream to understand the position where to append.
        /// </summary>
        /// <param name="appendRequests"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<IDictionary<string, BatchStreamAppendResult>> AppendAsync(
            IEnumerable<BatchAppendRequest> appendRequests,
            CancellationToken cancellationToken
        );
    }
}
