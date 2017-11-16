using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using NStore.Core.Persistence;

namespace NStore.LoadTests
{
    public interface IConsumer<in T>
    {
        Task<bool> ReceiveAsync(T msg);
    }

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

    public class IoTConsumer : AbstractService<DeviceMessage>,
        IConsumer<DeviceMessage>
    {
        private readonly IPersistence _persistence;
        public Task<bool> ReceiveAsync(DeviceMessage msg) => PushAsync(msg);

        public IoTConsumer(int workers, int bufferSize, IPersistence persistence) : base(workers, bufferSize)
        {
            _persistence = persistence;
        }

        protected override async Task ProcessAsync(DeviceMessage payload)
        {
            Track.Inc(Counters.ReceivedMessages);
            while (true)
            {
                try
                {
                    await _persistence.AppendAsync(
                        payload.DeviceId,
                        DateTime.UtcNow.Ticks,
                        payload
                    ).ConfigureAwait(false);
                    return;
                }
                catch (DuplicateStreamIndexException)
                {
                    // retry with new ticks
                }
            }
        }
    }

    public class IoTProducer : AbstractService<long>
    {
        private readonly IConsumer<DeviceMessage> _consumer;

        public IoTProducer(int workers, int bufferSize, IConsumer<DeviceMessage> consumer) : base(workers, bufferSize)
        {
            _consumer = consumer;
        }

        protected override async Task ProcessAsync(long payload)
        {
            var msg = new DeviceMessage()
            {
                Sequence = payload,
                DeviceId = $"Sensor{payload % 10}",
                Counter1 = payload % 121,
                Counter2 = payload / 2
            };
            await _consumer.ReceiveAsync(msg).ConfigureAwait(false);
            Track.Inc(Counters.SentMessages);
        }

        public  Task<bool> SimulateMessage(long msgId)
        {
            Track.Inc(Counters.SimulatedMessages);
            return PushAsync(msgId);
        }
    }
}