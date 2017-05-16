using System.Threading.Tasks;

namespace NStore.InMemory
{
    public interface INetworkSimulator
    {
        Task<long> WaitFast();
        Task<long> Wait();
    }
}