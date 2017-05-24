using System;

namespace NStore.Raw
{
    public class NullPartitionConsumer : IPartitionConsumer
    {
        public static readonly NullPartitionConsumer Instance = new NullPartitionConsumer();

        private NullPartitionConsumer()
        {
        }

        public ScanAction Consume(long partitionIndex, object payload)
        {
			return ScanAction.Continue;
		}
    }
}