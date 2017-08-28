using System.Threading.Tasks;

namespace NStore.Core.InMemory
{
    public interface INetworkSimulator
    {
        Task<long> WaitFast();
        Task<long> Wait();
    }
}