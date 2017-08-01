using System;
using System.Threading.Tasks;

namespace NStore.InMemory
{
    public class UnreliableNetworkSimulator : ReliableNetworkSimulator
    {
        public UnreliableNetworkSimulator() : this(100, 300)
        {
        }

        public UnreliableNetworkSimulator(int minDelayMs, int maxDelayMs) : base(minDelayMs, maxDelayMs)
        {
        }

        protected override async Task<long> Simulate(int max)
        {
            var elapsed = await base.Simulate(max).ConfigureAwait(false);

            if (elapsed % 7 == 0)
                throw new TimeoutException();

            return elapsed;
        }
    }
}