using System.Threading.Tasks;

namespace NStore.InMemory
{
    public class LocalhostSimulator : INetworkSimulator
    {
        public Task<long> WaitFast()
        {
            return Task.FromResult(1L);
        }

        public Task<long> Wait()
        {
            return Task.FromResult(2L);
        }
    }
}