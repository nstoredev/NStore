using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public interface IReadOnlyStream
    {
        Task Read(
            IPartitionConsumer partitionConsumer
        );
        
        Task Read(
            IPartitionConsumer partitionConsumer, 
            int fromIndexInclusive
        );

        Task Read(
            IPartitionConsumer partitionConsumer, 
            int fromIndexInclusive, 
            CancellationToken cancellationToken
        );  

        Task Read(
            IPartitionConsumer partitionConsumer, 
            int fromIndexInclusive, 
            int toIndexInclusive 
        );
        
        Task Read(
            IPartitionConsumer partitionConsumer, 
            int fromIndexInclusive, 
            int toIndexInclusive, 
            CancellationToken cancellationToken
        );
    }
}