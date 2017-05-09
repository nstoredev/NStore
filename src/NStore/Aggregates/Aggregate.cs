using System.Collections.Generic;
using System.Xml.Linq;

namespace NStore.Aggregates
{
    public abstract class Aggregate<TState> : IAggregate where TState : AggregateState, new()
    {
        public int Version { get; private set; }
        public bool IsInitialized { get; private set; }

        public IList<object> UncommittedEvents { get; private set; } = new List<object>();
        protected IEventDispatcher Dispatcher;
        protected TState State { get; private set; }

        protected Aggregate(IEventDispatcher dispatcher = null)
        {
            this.Dispatcher = dispatcher ?? new DefaultEventDispatcher<TState>(() => this.State);
        }

        void IAggregate.Init(int version, object state) => 
            Init(version, (TState) state);

        public void Init(int version = 0, TState state = null)
        {
            this.State = state ?? new TState();
            this.IsInitialized = true;
            this.UncommittedEvents.Clear();
            this.Version = version;
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