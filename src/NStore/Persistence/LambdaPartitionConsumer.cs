using System;
using System.Threading;
using System.Threading.Tasks;

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

        public Task<bool> OnNext(IPartitionData partitionData)
        {
            return this._fn(partitionData);
        }

        public Task Completed()
        {
            this.ReadCompleted = true;
            return Task.CompletedTask;
        }

        public Task OnError(Exception ex)
        {
            _failed = ex;
            return Task.CompletedTask;
        }
    }
}