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

    public static class ReadOnlyStreamExtensions
    {
        public static Task Read(this IReadOnlyStream stream, IPartitionConsumer partitionConsumer)
        {
            return stream.Read(partitionConsumer, 0, int.MaxValue, CancellationToken.None);
        }

        public static Task Read(this IReadOnlyStream stream, IPartitionConsumer partitionConsumer, int fromIndexInclusive)
        {
            return stream.Read(partitionConsumer, fromIndexInclusive, int.MaxValue, CancellationToken.None);
        }

        public static Task Read(this IReadOnlyStream stream, IPartitionConsumer partitionConsumer, int fromIndexInclusive, int toIndexInclusive)
        {
            return stream.Read(partitionConsumer, fromIndexInclusive, toIndexInclusive, CancellationToken.None);
        }
    }
}