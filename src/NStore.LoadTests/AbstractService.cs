using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace NStore.LoadTests
{
    public abstract class AbstractService<TPayload>
    {
        private readonly ActionBlock<TPayload> _queue;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        
        protected AbstractService(int workers, int bufferSize)
        {
            var options = new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = workers,
                BoundedCapacity = bufferSize,
                CancellationToken = _cts.Token
            };
            _queue = new ActionBlock<TPayload>(ProcessAsync, options);
        }
       
        public async Task FlushAndShutDown()
        {
            while (_queue.InputCount > 0)
            {
                await Task.Delay(100).ConfigureAwait(false);
            }
            _queue.Complete();
            await _queue.Completion.ConfigureAwait(false);
        }

        public Task Stop()
        {
            _cts.Cancel();
            return FlushAndShutDown();
        }

        protected Task<bool> PushAsync(TPayload payload)
        {
            return _queue.SendAsync(payload);
        }

        protected abstract Task ProcessAsync(TPayload payload);

    }
}