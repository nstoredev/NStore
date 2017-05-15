using System.Threading.Tasks;
using NStore.Streams;

namespace NStore.Aggregates
{
    public interface IAggregate
    {
        string Id { get; }
        int Version { get; }

        bool IsInitialized { get; }
        void Init(string id, int version = 0, object @state = null);
    }
}