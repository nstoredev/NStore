using System;

namespace NStore.Persistence
{
    public class LambdaPartitionConsumer : IPartitionConsumer
    {
        private readonly ProcessPartitionData _fn;
        private Exception _failed;
        public bool ReadCompleted { get; private set; }
        public bool Failed => this._failed != null;
        public Exception LastError => _failed;
        
        public LambdaPartitionConsumer(ProcessPartitionData fn)
        {
            _fn = fn;
        }

        public ScanAction Consume(IPartitionData partitionData)
        {
            return this._fn(partitionData);
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