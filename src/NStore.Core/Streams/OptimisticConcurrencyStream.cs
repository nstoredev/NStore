using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Core.Streams
{
    //@@REVIEW: can be refatored to a Decorator on Stream
    public class OptimisticConcurrencyStream : IStream
    {
        private IPersistence Persistence { get; }

        public long Version { get; private set; } = -1;
        public string Id { get; }
        public bool IsWritable => true;

        public OptimisticConcurrencyStream(string streamId, IPersistence persistence)
        {
            this.Id = streamId;
            this.Persistence = persistence;
        }

        public Task ReadAsync(ISubscription subscription, long fromIndexInclusive, long toIndexInclusive, CancellationToken cancellationToken)
        {
            // @@REVIEW: micro optimization for reading only last index? (fromIndexInclusive == toIndexInclusive == Int32.MaxValue)
            var readConsumer = subscription;
            if (toIndexInclusive == long.MaxValue)
            {
                Version = 0;
                readConsumer = new SubscriptionWrapper(subscription)
                {
                    BeforeOnNext = (data) => { Version = data.Index; }
                };
            }

            return Persistence.ReadForwardAsync(
                Id,
                fromIndexInclusive,
                readConsumer,
                toIndexInclusive,
                limit: Int32.MaxValue,
                cancellationToken: cancellationToken
            );
        }

        public async Task<bool> IsEmpty(CancellationToken cancellationToken)
        {
            // @@REVIEW: check version to avoid db rountrip
            return await Persistence.ReadSingleBackwardAsync(this.Id, cancellationToken).ConfigureAwait(false) != null;
        }

        public async Task<bool> ContainsOperationAsync(string operationId, CancellationToken cancellationToken)
        {
            var chunk = await Persistence.ReadByOperationIdAsync(this.Id, operationId, cancellationToken).ConfigureAwait(false);
            return chunk != null;
        }

        public async Task<IChunk> AppendAsync(object payload, string operationId, CancellationToken cancellation)
        {
			IChunk chunk = null;
            if (Version == -1)
                throw new AppendFailedException(this.Id,
                        $@"Cannot append on stream {this.Id}
Append can be called only after a Read operation.
If you don't need to read use {typeof(Stream).Name} instead of {GetType().Name}.")
                    ;
            long desiredVersion = this.Version + 1;
            try
            {
				chunk = await Persistence.AppendAsync(this.Id, desiredVersion, payload, operationId, cancellation).ConfigureAwait(false);
            }
            catch (DuplicateStreamIndexException e)
            {
                throw new ConcurrencyException($"Concurrency exception on StreamId: {this.Id}", e);
            }
            this.Version = desiredVersion;
			return chunk;
		}

        public Task DeleteAsync(CancellationToken cancellation)
        {
            return Persistence.DeleteAsync(this.Id, 0, long.MaxValue, cancellation);
        }
    }
}