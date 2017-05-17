using System.Threading.Tasks;
using NStore.Streams;

namespace NStore.Aggregates
{
    public interface IAggregate
    {
        string Id { get; }
        int Version { get; }

        bool IsInitialized { get; }
        bool IsDirty { get; }
        bool IsNew { get; }
        void Init(string id);
    }
}