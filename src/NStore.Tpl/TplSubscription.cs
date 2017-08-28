using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using NStore.Core.Persistence;

namespace NStore.Tpl
{
    public class TplSubscription : ISubscription
    {
        private readonly ISubscription _consumer;
        private readonly ActionBlock<IChunk> _producer;

        private bool _isRunning;

        public TplSubscription(ISubscription consumer, CancellationToken token)
        {
            _consumer = consumer;
            _producer = new ActionBlock<IChunk>(Process, new ExecutionDataflowBlockOptions()
            {
                CancellationToken = token,
                SingleProducerConstrained = true
            });
            _isRunning = true;
        }

        private async Task Process(IChunk chunk)
        {
            if (_isRunning)
            {
                _isRunning = await _consumer.OnNextAsync(chunk).ConfigureAwait(false);
            }
        }

        public Task OnStartAsync(long indexOrPosition)
        {
            return Task.CompletedTask;
        }

        public async Task<bool> OnNextAsync(IChunk chunk)
        {
            await _producer.SendAsync(chunk).ConfigureAwait(false);
            return _isRunning;
        }

        public async Task CompletedAsync(long indexOrPosition)
        {
            _producer.Complete();
            await _producer.Completion.ConfigureAwait(false);
            _isRunning = false;
        }

        public async Task StoppedAsync(long indexOrPosition)
        {
            _producer.Complete();
            await _producer.Completion.ConfigureAwait(false);
            _isRunning = false;
        }

        public Task OnErrorAsync(long indexOrPosition, Exception ex)
        {
            _isRunning = false;
            return Task.CompletedTask;
        }
    }
}
