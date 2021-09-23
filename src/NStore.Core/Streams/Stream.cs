using NStore.Core.Persistence;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Streams
{
    public class Stream : IRandomAccessStream
    {
        private IPersistence Persistence { get; }
        public string Id { get; }
        public virtual bool IsWritable => true;
        private long _lastIndex = -1;

        public Stream(string streamId, IPersistence persistence)
        {
            this.Id = streamId;
            this.Persistence = persistence;
        }

        public Task ReadAsync(
            ISubscription subscription,
            long fromIndexInclusive,
            long toIndexInclusive,
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

        public virtual async Task<IChunk> AppendAsync(
            object payload,
            string operationId,
            CancellationToken cancellation
        )
        {
            for (var retries = 0; retries < 10; retries++)
            {
                try
                {
                    if (_lastIndex == -1)
                    {
                        var last = await PeekAsync(cancellation).ConfigureAwait(false);
                        _lastIndex = last?.Index ?? 0;
                    }

                    var index = _lastIndex + 1;

                    var chunk = await Persistence.AppendAsync(this.Id, index, payload, operationId, cancellation)
                        .ConfigureAwait(false);

                    _lastIndex = chunk.Index;
                    return chunk;
                }
                catch (DuplicateStreamIndexException)
                {
                    _lastIndex = -1;
                }
            }

            throw new AppendFailedException(this.Id, "Too many retries");
        }

        public virtual Task DeleteAsync(CancellationToken cancellation)
        {
            return Persistence.DeleteAsync(this.Id, 0, long.MaxValue, cancellation);
        }

        public Task DeleteBeforeAsync(long index, CancellationToken cancellation)
        {
            return Persistence.DeleteAsync(this.Id, 0, index - 1, cancellation);
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