using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public class OptimisticConcurrencyStream : IStream
    {
        public long Version { get; private set; } = -1;
        protected IRawStore Raw { get; }
        public string Id { get; }

        public OptimisticConcurrencyStream(string streamId, IRawStore raw)
        {
            this.Id = streamId;
            this.Raw = raw;
        }

        public Task Read(IPartitionObserver partitionObserver, int fromIndexInclusive, int toIndexInclusive, CancellationToken cancellationToken = default(CancellationToken))
        {
            // @@TODO: micro optimization for reading only last index? (fromIndexInclusive == toIndexInclusive == Int32.MaxValue)
            var readConsumer = partitionObserver;
            if (toIndexInclusive == Int32.MaxValue)
            {
                Version = 0;
                readConsumer = new LambdaPartitionObserver((l, o) =>
                {
                    Version = l;
                    return partitionObserver.Observe(l, o);
                });
            }

            return Raw.ScanPartitionAsync(
                Id,
                fromIndexInclusive,
                ScanDirection.Forward,
                readConsumer,
                toIndexInclusive,
                cancellationToken: cancellationToken
            );
        }

        public async Task Append(object payload, string operationId, CancellationToken cancellation = default(CancellationToken))
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

        public Task Delete(CancellationToken cancellation = default(CancellationToken))
        {
            return Raw.DeleteAsync(this.Id, cancellationToken: cancellation);
        }

        //@@TODO Refator => delete?
        protected async Task<long> LoadActualVersion(CancellationToken cancellation = default(CancellationToken))
        {
            var tape = new Tape();
            await Raw.ScanPartitionAsync(
                this.Id,
                0,
                ScanDirection.Backward,
                tape,
                Int64.MaxValue,
                1,
                cancellation
            );

            if (tape.IsEmpty)
                return 0;

            return tape.GetIndex(0);
        }
    }
}