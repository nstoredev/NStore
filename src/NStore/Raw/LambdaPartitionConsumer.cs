using System;

namespace NStore.Raw
{
    public class LambdaPartitionConsumer : IPartitionConsumer
    {
        private readonly Func<long, object, ScanAction> _fn;

        public LambdaPartitionConsumer(Func<long, object, ScanAction> fn)
        {
            _fn = fn;
        }

        public ScanAction Consume(long partitionIndex, object payload)
        {
            return this._fn(partitionIndex, payload);
        }
    }
}