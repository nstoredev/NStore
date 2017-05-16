using System;
using System.Threading.Tasks;

namespace NStore.InMemory
{
    public class CloudSimulator : LocalAreaNetworkSimulator
    {
        public CloudSimulator(int maxDelayMs = 100) : base(maxDelayMs)
        {
        }

        protected override async Task<long> Simulate(int max)
        {
            var elapsed = await base.Simulate(max);

            if (elapsed % 7 == 0)
                throw new TimeoutException();

            return elapsed;
        }
    }
}