using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class LambdaSubscription : ISubscription
    {
        private readonly StreamDataProcessor _fn;
        private Exception _failed;
        public bool ReadCompleted { get; private set; }
        public bool Failed => this._failed != null;
        public Exception LastError => _failed;
        
        public LambdaSubscription(StreamDataProcessor fn)
        {
            _fn = fn;
        }

        public Task<bool> OnNext(IChunk chunk)
        {
            return this._fn(chunk);
        }

        public Task OnStart(long position)
        {
            return Task.CompletedTask;
        }

        public Task Completed(long position)
        {
            this.ReadCompleted = true;
            return Task.CompletedTask;
        }

        public Task Stopped(long position)
        {
            this.ReadCompleted = true;
            return Task.CompletedTask;
        }

        public Task OnError(long position, Exception ex)
        {
            _failed = ex;
            return Task.CompletedTask;
        }
    }
}