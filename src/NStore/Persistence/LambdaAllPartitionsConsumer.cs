using System;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class LambdaAllPartitionsConsumer : IAllPartitionsConsumer
    {
        private readonly Func<long, string, long, object, Task<ScanAction>> _fn;

        public LambdaAllPartitionsConsumer(Func<long, string, long, object, Task<ScanAction>> fn)
        {
            _fn = fn;
        }

        public Task<ScanAction> Consume(
            long position, 
            string partitionId, 
            long index, 
            object payload)
        {
            return this._fn(position, partitionId, index, payload);
        }
    }
}