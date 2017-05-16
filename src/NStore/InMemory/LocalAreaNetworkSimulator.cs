using System;
using System.Threading.Tasks;

namespace NStore.InMemory
{
    public class LocalAreaNetworkSimulator : INetworkSimulator
    {
        private readonly int _randomMaxMs;
        private readonly int _minDelayMs;
        private readonly Random _random = new Random(DateTime.UtcNow.Millisecond * 7);

        public LocalAreaNetworkSimulator(int minDelayMs = 50, int maxDelayMs = 100)
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
            await Task.Delay((int)ms);
            return ms;
        }
    }
}