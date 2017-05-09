using System;

namespace NStore.Raw
{
    public class LambdaConsumer : IConsumer
    {
        private readonly Func<long, object, ScanCallbackResult> _fn;

        public LambdaConsumer(Func<long, object, ScanCallbackResult> fn)
        {
            _fn = fn;
        }

        public ScanCallbackResult Consume(long partitionIndex, object payload)
        {
            return this._fn(partitionIndex, payload);
        }
    }
}