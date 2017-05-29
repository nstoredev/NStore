using System;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class LambdaStoreConsumer : IStoreConsumer
    {
        private readonly Func<long, string, long, object, Task<ScanAction>> _fn;

        public LambdaStoreConsumer(Func<long, string, long, object, Task<ScanAction>> fn)
        {
            _fn = fn;
        }

        public Task<ScanAction> Consume(
            long storeIndex, 
            string streamId, 
            long partitionIndex, 
            object payload)
        {
            return this._fn(storeIndex, streamId, partitionIndex, payload);
        }
    }
}