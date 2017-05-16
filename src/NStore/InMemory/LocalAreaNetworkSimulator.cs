using System;
using System.Threading.Tasks;

namespace NStore.InMemory
{
    public class LocalAreaNetworkSimulator : INetworkSimulator
    {
        private readonly int _maxDelayMs;
        private readonly Random _random = new Random(DateTime.UtcNow.Millisecond * 7);

        public LocalAreaNetworkSimulator(int maxDelayMs = 100)
        {
            _maxDelayMs = maxDelayMs;
        }

        public Task<long> WaitFast()
        {
            return Simulate(_maxDelayMs / 2);
        }

        public Task<long> Wait()
        {
            return Simulate(_maxDelayMs);
        }

        protected virtual async Task<long> Simulate(int maxDelay)
        {
            var ms = _random.Next(maxDelay);
            await Task.Delay((int) ms);
            return ms;
        }
    }
}