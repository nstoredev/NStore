using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using System;

namespace NStore.Core.Streams
{
    public class Stream : IRandomAccessStream
    {
        private IPersistence Persistence { get; }
        public string Id { get; }
        public virtual bool IsWritable => true;

        public Stream(string streamId, IPersistence persistence)
        {
            this.Id = streamId;
            this.Persistence = persistence;
        }

        public Task ReadAsync(ISubscription subscription, long fromIndexInclusive, long toIndexInclusive,
            CancellationToken cancellationToken)
        {
            return Persistence.ReadForwardAsync(
                Id,
                fromIndexInclusive,
                subscription,
                toIndexInclusive,
                int.MaxValue,
                cancellationToken
            );
        }

        public Task<IChunk> PeekAsync(CancellationToken cancellationToken)
        {
            return Persistence.ReadSingleBackwardAsync(Id, cancellationToken);
        }

        public virtual Task<IChunk> AppendAsync(
            object payload,
            string operationId,
            CancellationToken cancellation
            )
        {
            return Persistence.AppendAsync(this.Id, -1, payload, operationId, cancellation);
        }

        public virtual Task DeleteAsync(CancellationToken cancellation)
        {
            return Persistence.DeleteAsync(this.Id, 0, long.MaxValue, cancellation);
        }

        public Task<IChunk> PersistAsync(object payload, long index, string operationId, CancellationToken cancellation)
        {
            return Persistence.AppendAsync(this.Id, index, payload, operationId, cancellation);
        }

        public async Task<bool> IsEmpty(CancellationToken cancellationToken)
        {
            return await Persistence.ReadSingleBackwardAsync(this.Id, cancellationToken).ConfigureAwait(false) == null;
        }

        public async Task<bool> ContainsOperationAsync(string operationId, CancellationToken cancellationToken)
        {
            var chunk = await Persistence.ReadByOperationIdAsync(this.Id, operationId, cancellationToken)
                .ConfigureAwait(false);
            return chunk != null;
        }
    }
}