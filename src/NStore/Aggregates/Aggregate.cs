using System;
using System.Collections.Generic;
using System.Linq;
using NStore.SnapshotStore;

namespace NStore.Aggregates
{
    public abstract class Aggregate<TState> :
        IAggregatePersister,
        IAggregate
        where TState : AggregateState, new()
    {
        public string Id { get; private set; }
        public int Version { get; private set; }
        public bool IsInitialized { get; private set; }

        private IList<object> PendingChanges { get; } = new List<object>();
        protected readonly IEventDispatcher Dispatcher;
        protected TState State { get; private set; }
        public bool IsDirty => this.PendingChanges.Any();
        public bool IsNew => this.Version == 0;

        protected Aggregate(IEventDispatcher dispatcher = null)
        {
            this.Dispatcher = dispatcher ?? new DefaultEventDispatcher<TState>(() => this.State);
        }

        void IAggregate.Init(string id, int version, object state) =>
            Init(id, version, (TState)state);

        public void Init(string id, int version = 0, TState state = null)
        {
            if (String.IsNullOrEmpty(id))
                throw new ArgumentNullException(nameof(id));

            if (this.Id != null)
                throw new AggregateAlreadyInitializedException(GetType(), this.Id);

            this.Id = id;
            this.State = state ?? new TState();
            this.IsInitialized = true;
            this.PendingChanges.Clear();
            this.Version = version;
        }

        SnapshotInfo IAggregatePersister.GetSnapshot()
        {
            return new SnapshotInfo(this.Version, this.State);
        }

        void IAggregatePersister.ChangesPersisted(Changeset changeset)
        {
            this.Version = changeset.Version;
            this.PendingChanges.Clear();
        }

        void IAggregatePersister.ApplyChanges(Changeset changeset)
        {
            this.Version = changeset.Version;
            foreach (var @event in changeset.Events)
            {
                this.Dispatch(@event);
            }
        }

        Changeset IAggregatePersister.GetChangeSet()
        {
            return new Changeset(
                this.Version + 1,
                this.PendingChanges.ToArray()
            );
        }

        protected void Dispatch(object @event)
        {
            this.Dispatcher.Dispatch(@event);
        }

        protected void Raise(object @event)
        {
            this.PendingChanges.Add(@event);
            this.Dispatch(@event);
        }
    }
}