using System;

namespace NStore.Persistence
{
    public class LambdaPartitionConsumer : IPartitionConsumer
    {
        private readonly Func<long, object, ScanAction> _fn;
        private Exception _failed;
        public bool ReadCompleted { get; private set; }
        public bool Failed => this._failed != null;
        public Exception LastError => _failed;
        
        public LambdaPartitionConsumer(Func<long, object, ScanAction> fn)
        {
            _fn = fn;
        }

        public ScanAction Consume(long partitionIndex, object payload)
        {
            return this._fn(partitionIndex, payload);
        }

        public void Completed()
        {
            this.ReadCompleted = true;
        }

        public void OnError(Exception ex)
        {
            _failed = ex;
        }
    }
}