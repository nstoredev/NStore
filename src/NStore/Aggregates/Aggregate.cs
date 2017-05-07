using System.Collections.Generic;
using System.Xml.Linq;

namespace NStore.Aggregates
{
    public abstract class Aggregate<TState> : IAggregate where TState : AggregateState, new()
    {
        public int Version { get; private set; }

        //@@TODO check semantic: initialized == State != null?
        public bool IsInitialized => Version > 0;

        public IList<object> UncommittedEvents { get; private set; } = new List<object>();
        protected IEventDispatcher Dispatcher;
        protected TState State { get; private set; }

        protected Aggregate(IEventDispatcher dispatcher = null)
        {
            this.Dispatcher = dispatcher ?? new DefaultEventDispatcher<TState>(() => this.State);
        }

        void IAggregate.Init(object state) => Init((TState) state);

        public void Init(TState state = null)
        {
            this.State = state ?? new TState();
        }

        public void Append(object @event)
        {
            this.Version++;
            this.Dispatcher.Dispatch(@event);
        }

        protected void Raise(object @event)
        {
            this.UncommittedEvents.Add(@event);
            Append(@event);
        }
    }
}