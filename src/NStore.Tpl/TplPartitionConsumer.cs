using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using NStore.Persistence;

namespace NStore.Tpl
{
    public class TplPartitionConsumer : IPartitionConsumer
    {
        private readonly IPartitionConsumer _consumer;
        private readonly ActionBlock<IPartitionData> _producer;

        private bool _isRunning;

        public TplPartitionConsumer(IPartitionConsumer consumer, CancellationToken token)
        {
            _consumer = consumer;
            _producer = new ActionBlock<IPartitionData>(Process, new ExecutionDataflowBlockOptions()
            {
                CancellationToken = token,
                SingleProducerConstrained = true
            });
            _isRunning = true;
        }

        private async Task Process(IPartitionData partitionData)
        {
            if (_isRunning)
            {
                _isRunning = await _consumer.OnNext(partitionData).ConfigureAwait(false);
            }
        }

        public async Task<bool> OnNext(IPartitionData data)
        {
            await _producer.SendAsync(data).ConfigureAwait(false);
            return _isRunning;
        }

        public async Task Completed()
        {
            _producer.Complete();
            await _producer.Completion.ConfigureAwait(false);
            _isRunning = false;
        }

        public Task OnError(Exception ex)
        {
            _isRunning = false;
            return Task.CompletedTask;
        }
    }
}
