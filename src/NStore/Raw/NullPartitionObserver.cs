using System;

namespace NStore.Raw
{
    public class NullPartitionObserver : IPartitionObserver
    {
        public static readonly NullPartitionObserver Instance = new NullPartitionObserver();

        private NullPartitionObserver()
        {
        }

        public ScanCallbackResult Observe(long partitionIndex, object payload)
        {
			return ScanCallbackResult.Continue;
		}
    }
}