using System;

namespace NStore.Raw
{
    public class LambdaPartitionObserver : IPartitionObserver
    {
        private readonly Func<long, object, ScanCallbackResult> _fn;

        public LambdaPartitionObserver(Func<long, object, ScanCallbackResult> fn)
        {
            _fn = fn;
        }

        public ScanCallbackResult Observe(long partitionIndex, object payload)
        {
            return this._fn(partitionIndex, payload);
        }
    }
}