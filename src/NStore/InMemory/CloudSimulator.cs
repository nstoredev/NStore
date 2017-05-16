using System;
using System.Threading.Tasks;

namespace NStore.InMemory
{
    public class CloudSimulator : LocalAreaNetworkSimulator
    {
        public CloudSimulator(int maxDelayMs = 100) : base(maxDelayMs)
        {
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