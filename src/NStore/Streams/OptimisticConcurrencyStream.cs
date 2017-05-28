using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    //@@REVIEW: can be refatored to a Decorator on Stream
    public class OptimisticConcurrencyStream : IStream
    {
        private IRawStore Raw { get; }

        public long Version { get; private set; } = -1;
        public string Id { get; }
        public bool IsWritable => true;

        public OptimisticConcurrencyStream(string streamId, IRawStore raw)
        {
            this.Id = streamId;
            this.Raw = raw;
        }

        public Task Read(IPartitionConsumer partitionConsumer, int fromIndexInclusive, int toIndexInclusive,
            CancellationToken cancellationToken)
        {
            // @@REVIEW: micro optimization for reading only last index? (fromIndexInclusive == toIndexInclusive == Int32.MaxValue)
            var readConsumer = partitionConsumer;
            if (toIndexInclusive == Int32.MaxValue)
            {
                Version = 0;
                readConsumer = new LambdaPartitionConsumer((l, o) =>
                {
                    Version = l;
                    return partitionConsumer.Consume(l, o);
                });
            }

            return Raw.ReadPartitionForward(
                Id,
                fromIndexInclusive,
                readConsumer,
                toIndexInclusive,
                limit: Int32.MaxValue,
                cancellationToken: cancellationToken
            );
        }

        public async Task Append(object payload, string operationId, CancellationToken cancellation)
        {
            if (Version == -1)
                throw new AppendFailedException(this.Id,
                        $@"Cannot append on stream {this.Id}
Append can be called only after a Read operation.
If you don't need to read use {typeof(Stream).Name} instead of {GetType().Name}.")
                    ;
            long desiredVersion = this.Version + 1;
            await Raw.PersistAsync(this.Id, desiredVersion, payload, operationId, cancellation);
            this.Version = desiredVersion;
        }

        public Task Delete(CancellationToken cancellation)
        {
            return Raw.DeleteAsync(this.Id, 0, long.MaxValue, cancellation);
        }
    }
}