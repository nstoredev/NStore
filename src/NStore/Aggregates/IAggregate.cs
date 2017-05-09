using System.Threading.Tasks;
using NStore.Streams;

namespace NStore.Aggregates
{
    public interface IAggregate
    {
        string Id { get; }
        long Version { get; }

        bool IsInitialized { get; }
        void Init(string id, long version = 0, object @state = null);
    }

    public interface IAggregatePersister
    {
        void Append(Commit commit);
        Commit BuildCommit();
    }

    public static class AggregateExtensions
    {
        public static bool IsNew(this IAggregate aggregate) =>
            aggregate.IsInitialized && aggregate.Version == 0;
    }
}