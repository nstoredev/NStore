using System;

namespace NStore.Raw
{
    public class LambdaStoreConsumer : IStoreConsumer
    {
        private readonly Func<long, string, long, object, ScanAction> _fn;

        public LambdaStoreConsumer(Func<long, string, long, object, ScanAction> fn)
        {
            _fn = fn;
        }

        public ScanAction Consume(
            long storeIndex, 
            string streamId, 
            long partitionIndex, 
            object payload)
        {
            return this._fn(storeIndex, streamId, partitionIndex, payload);
        }
    }
}