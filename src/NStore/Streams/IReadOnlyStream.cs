using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;

namespace NStore.Streams
{
    public interface IReadOnlyStream
    {
        Task Read(
            IPartitionConsumer partitionConsumer,
            int fromIndexInclusive,
            int toIndexInclusive,
            CancellationToken cancellationToken
        );
    }
}