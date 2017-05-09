using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace NStore.Aggregates
{
    public abstract class Aggregate<TState> :
        IAggregatePersister,
        IAggregate
        where TState : AggregateState, new()
    {
        public string Id { get; private set; }
        public long Version { get; private set; }
        public bool IsInitialized { get; private set; }

        public IList<object> UncommittedEvents { get; private set; } = new List<object>();
        protected IEventDispatcher Dispatcher;
        protected TState State { get; private set; }

        protected Aggregate(IEventDispatcher dispatcher = null)
        {
            this.Dispatcher = dispatcher ?? new DefaultEventDispatcher<TState>(() => this.State);
        }

        void IAggregate.Init(string id, long version, object state) =>
            Init(id, version, (TState)state);

        public void Init(string id, long version = 0, TState state = null)
        {
            this.Id = id;
            this.State = state ?? new TState();
            this.IsInitialized = true;
            this.UncommittedEvents.Clear();
            this.Version = version;
        }

        void IAggregatePersister.AppendCommit(Commit commit)
        {
            this.Version = commit.Version;
            foreach (var @event in commit.Events)
            {
                this.Dispatch(@event);
            }
        }

        Commit IAggregatePersister.BuildCommit()
        {
            var commit = new Commit(
                this.Version + 1,
                this.UncommittedEvents.ToArray()
            );
            return commit;
        }

        protected void Dispatch(object @event)
        {
            this.Dispatcher.Dispatch(@event);
        }

        protected void Raise(object @event)
        {
            this.UncommittedEvents.Add(@event);
            this.Dispatch(@event);
        }
    }
}