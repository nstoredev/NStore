using NStore.Processing;

namespace NStore.Aggregates
{
    public abstract class AggregateState : PayloadProcessor
    {
        public virtual int GetStateVersion() => 1;
    }
}