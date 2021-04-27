using NStore.Core.InMemory;
using System;
using System.Threading.Tasks;

namespace NStore.Domain.Tests
{
    class AlwaysThrowsNetworkSimulator : INetworkSimulator
    {
        public Boolean ShouldThrow { get; set; }

        public Task<long> Wait()
        {
            if (ShouldThrow) throw new TimeoutException();

            return Task.FromResult(1L);
        }

        public Task<long> WaitFast()
        {
            if (ShouldThrow) throw new TimeoutException();

            return Task.FromResult(1L);
        }
    }
}
