using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public class LambdaSubscription : ISubscription
    {
        public delegate Task OnErrorDelegate(long position, Exception ex, CancellationToken cancellationToken);
        public delegate Task OnStartDelegate(long position, CancellationToken cancellationToken);
        public delegate Task OnCompleteDelegate(long position, CancellationToken cancellationToken);
        public delegate Task OnStopDelegate(long position, CancellationToken cancellationToken);

        private readonly ChunkProcessor _fn;
        private Exception _failed;
        private long _failedPosition;

        public bool ReadCompleted { get; private set; }
        public bool Failed => this._failed != null;
        public long FailedPosition => _failedPosition;
        public Exception LastError => _failed;

        public OnErrorDelegate OnError { get; set; }
        public OnStartDelegate OnStart { get; set; }
        public OnCompleteDelegate OnComplete { get; set; }
        public OnStopDelegate OnStop { get; set; }

        public LambdaSubscription(ChunkProcessor fn)
        {
            _fn = fn;
        }

        public Task<bool> OnNextAsync(IChunk chunk, CancellationToken cancellationToken)
        {
            return this._fn(chunk);
        }

        public Task OnStartAsync(long indexOrPosition, CancellationToken cancellationToken)
        {
            return OnStart != null
                ? OnStart(indexOrPosition, cancellationToken)
                : Task.CompletedTask;
        }

        public Task CompletedAsync(long indexOrPosition, CancellationToken cancellationToken)
        {
            this.ReadCompleted = true;

            return OnComplete != null
                ? OnComplete(indexOrPosition, cancellationToken)
                : Task.CompletedTask;
        }

        public Task StoppedAsync(long indexOrPosition, CancellationToken cancellationToken)
        {
            this.ReadCompleted = true;
            return OnStop != null
                ? OnStop(indexOrPosition, cancellationToken)
                : Task.CompletedTask;
        }

        public Task OnErrorAsync(long indexOrPosition, Exception ex, CancellationToken cancellationToken)
        {
            _failed = ex;
            _failedPosition = indexOrPosition;

            return OnError != null
                ? OnError(indexOrPosition, ex, cancellationToken)
                : Task.CompletedTask;
        }
    }
}
