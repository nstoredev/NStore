using System;

namespace NStore.Raw
{
    public class LambdaStoreObserver : IStoreObserver
    {
        private readonly Func<long, string, long, object, ScanCallbackResult> _fn;

        public LambdaStoreObserver(Func<long, string, long, object, ScanCallbackResult> fn)
        {
            _fn = fn;
        }

        public ScanCallbackResult Observe(long storeIndex, string streamId, long partitionIndex, object payload)
        {
            return this._fn(storeIndex, streamId, partitionIndex, payload);
        }
    }
}