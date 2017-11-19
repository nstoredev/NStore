using System;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.LoadTests
{
    public class Ingestor : AbstractService<DeviceMessage>,
        IConsumer<DeviceMessage>
    {
        private readonly IPersistence _persistence;
        public Task<bool> ReceiveAsync(DeviceMessage msg) => PushAsync(msg);

        public Ingestor(int workers, int bufferSize, IPersistence persistence) : base(workers, bufferSize)
        {
            _persistence = persistence;
        }

        protected override async Task ProcessAsync(DeviceMessage payload)
        {
            Track.Inc(Counters.ReceivedMessages);
            await Track.Profile(Timers.RequestTimer, async () =>
            {
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
            }).ConfigureAwait(false);
        }
    }
}