using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Core.Streams
{
    //@@REVIEW: could be refactored as a Decorator on Stream?
    public class OptimisticConcurrencyStream : IStream
    {
        private IPersistence Persistence { get; }

        private long _version = -1;
        public string Id { get; }
        public bool IsWritable => true;

        public OptimisticConcurrencyStream(
            string streamId,
            IPersistence persistence
        )
        {
            Id = streamId;
            Persistence = persistence;
        }

        public Task ReadAsync(
            ISubscription subscription,
            long fromIndexInclusive,
            long toIndexInclusive,
            CancellationToken cancellationToken
        )
        {
            // @@REVIEW: micro optimization for reading only last index? (fromIndexInclusive == toIndexInclusive == Int32.MaxValue)
            var readConsumer = subscription;
            if (toIndexInclusive == long.MaxValue)
            {
                _version = 0;
                readConsumer = new SubscriptionWrapper(subscription)
                {
                    BeforeOnNext = (data) => { _version = data.Index; }
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

        public Task<IChunk> PeekAsync(CancellationToken cancellationToken)
        {
            return Persistence.ReadSingleBackwardAsync(Id, cancellationToken);
        }

        public async Task<bool> IsEmpty(CancellationToken cancellationToken)
        {
            // @@REVIEW: check version to avoid db rountrip
            return await Persistence.ReadSingleBackwardAsync(Id, cancellationToken)
                       .ConfigureAwait(false) != null;
        }

        public async Task<bool> ContainsOperationAsync(
            string operationId,
            CancellationToken cancellationToken
        )
        {
            var chunk = await Persistence.ReadByOperationIdAsync(
                Id,
                operationId,
                cancellationToken
            ).ConfigureAwait(false);

            return chunk != null;
        }

        public async Task<IChunk> AppendAsync(
            object payload,
            long index,
            string operationId,
            CancellationToken cancellation
        )
        {
            if (index != -1)
            {
                throw new AppendFailedException(Id,
                    $@"Cannot append on stream {Id}
Append can't be called with an index value.");
            }

            IChunk chunk = null;
            if (_version == -1)
            {
                throw new AppendFailedException(Id,
                    $@"Cannot append on stream {Id}
Append can be called only after a Read operation.
If you don't need to read use {typeof(Stream).Name} instead of {GetType().Name}.");
            }

            long desiredVersion = _version + 1;
            try
            {
                chunk = await Persistence.AppendAsync(
                    Id,
                    desiredVersion,
                    payload,
                    operationId,
                    cancellation
                ).ConfigureAwait(false);
            }
            catch (DuplicateStreamIndexException e)
            {
                throw new ConcurrencyException($"Concurrency exception on StreamId: {this.Id}", e);
            }

            _version = desiredVersion;
            return chunk;
        }

        public Task DeleteAsync(CancellationToken cancellation)
        {
            return Persistence.DeleteAsync(Id, 0, long.MaxValue, cancellation);
        }
    }
}