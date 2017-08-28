using System;
using System.Threading.Tasks;

namespace NStore.Core.InMemory
{
    public class ReliableNetworkSimulator : INetworkSimulator
    {
        private readonly int _randomMaxMs;
        private readonly int _minDelayMs;
        private readonly Random _random = new Random(DateTime.UtcNow.Millisecond * 7);

        public ReliableNetworkSimulator() : this(50, 100)
        {
        }

        public ReliableNetworkSimulator(int minDelayMs, int maxDelayMs)
        {
            _minDelayMs = minDelayMs;
            _randomMaxMs = maxDelayMs - minDelayMs;
        }

        public Task<long> WaitFast()
        {
            return Simulate(_randomMaxMs / 4);
        }

        public Task<long> Wait()
        {
            return Simulate(_randomMaxMs);
        }

        protected virtual async Task<long> Simulate(int max)
        {
            var ms = _minDelayMs + _random.Next(max);
            await Task.Delay(ms).ConfigureAwait(false);
            return ms;
        }
    }
}