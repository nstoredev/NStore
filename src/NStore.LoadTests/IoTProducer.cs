using System.Threading.Tasks;

namespace NStore.LoadTests
{
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