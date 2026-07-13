using System;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public interface ISubscription
    {
        /// <summary>
        /// <para>
        /// Called when reading of <see cref="IChunk"/> starts, it can be used to initialize
        /// the component.
        /// </para>
        /// </summary>
        /// <param name="indexOrPosition"></param>
        /// <returns></returns>
        Task OnStartAsync(long indexOrPosition);

        /// <summary>
        /// <para>
        /// Handles the next <see cref="IChunk"/> in the persistence layer.
        /// Normal task completion acknowledges the current chunk. Return
        /// <see cref="Subscription.Continue"/> to acknowledge it and continue reading, or
        /// <see cref="Subscription.Stop"/> to acknowledge it and stop before the next chunk.
        /// Fault or cancel the task when the current chunk was not processed successfully.
        /// A polling caller can then retain its previous position and redeliver the chunk.
        /// Consumers must not swallow transient failures or advance durable checkpoints before
        /// all required side effects have completed.
        /// </para>
        /// <para>
        /// Delivery can be repeated when the processing outcome is ambiguous, so consumers
        /// should use a stable chunk identity and make side effects idempotent.
        /// </para>
        /// </summary>
        /// <param name="chunk"></param>
        /// <returns>The acknowledgement and continuation decision for the current chunk.</returns>
        Task<bool> OnNextAsync(IChunk chunk);

        /// <summary>
        /// <para>
        /// Called when the reading is complete.
        /// </para>
        /// </summary>
        /// <param name="indexOrPosition"></param>
        /// <returns></returns>
        Task CompletedAsync(long indexOrPosition);

        /// <summary>
        /// <para>
        /// Called when reading of chunks stops, it is usually determines by the return
        /// value of the <see cref="OnNextAsync(IChunk)"/>.
        /// </para>
        /// </summary>
        /// <param name="indexOrPosition"></param>
        /// <returns></returns>
        Task StoppedAsync(long indexOrPosition);

        /// <summary>
        /// <para>
        /// Called when there is an exception reading or dispatching the next chunk. The real
        /// concrete subscription can then take any action it determines to be done to recover
        /// from the error. The failed chunk has not been acknowledged by
        /// <see cref="PollingClient"/> and can be delivered again. Implementations must not
        /// advance a durable checkpoint for that chunk from this callback.
        /// </para>
        /// </summary>
        /// <param name="indexOrPosition"></param>
        /// <param name="ex"></param>
        /// <returns></returns>
        Task OnErrorAsync(long indexOrPosition, Exception ex);
    }

    public delegate Task<bool> ChunkProcessor(IChunk chunk);
}
