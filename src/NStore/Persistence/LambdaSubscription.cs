using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class LambdaSubscription : ISubscription
    {
        private readonly ChunkProcessor _fn;
        private Exception _failed;
        public bool ReadCompleted { get; private set; }
        public bool Failed => this._failed != null;
        public Exception LastError => _failed;
        
        public LambdaSubscription(ChunkProcessor fn)
        {
            _fn = fn;
        }

        public Task<bool> OnNextAsync(IChunk chunk)
        {
            return this._fn(chunk);
        }

        public Task OnStartAsync(long position)
        {
            return Task.CompletedTask;
        }

        public Task CompletedAsync(long position)
        {
            this.ReadCompleted = true;
            return Task.CompletedTask;
        }

        public Task StoppedAsync(long position)
        {
            this.ReadCompleted = true;
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(long position, Exception ex)
        {
            _failed = ex;
            return Task.CompletedTask;
        }
    }
}