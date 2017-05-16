using System;
using System.Threading.Tasks;

namespace NStore.InMemory
{
    public class CloudSimulator : LocalAreaNetworkSimulator
    {
        private readonly int _maxDelayMs;
        private readonly Random _random = new Random(DateTime.UtcNow.Millisecond * 7);
        public CloudSimulator(int maxDelayMs = 1000)
        {
            _maxDelayMs = maxDelayMs;
        }

        protected override async Task<long> Simulate(int maxDelay)
        {
            var elapsed = await base.Simulate(maxDelay);

            if (elapsed % 7 == 0)
                throw new TimeoutException();

            return elapsed;
        }
    }
}