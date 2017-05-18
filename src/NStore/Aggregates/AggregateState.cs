namespace NStore.Aggregates
{
    public abstract class AggregateState : EventsProjector
    {
        public virtual int GetStateVersion() => 1;
    }
}