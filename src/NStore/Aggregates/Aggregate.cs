using System;
using System.Collections.Generic;
using System.Linq;
using NStore.SnapshotStore;

namespace NStore.Aggregates
{
    public abstract class Aggregate<TState> :
        IEventSourcedAggregate,
        IAggregate
        where TState : AggregateState, new()
    {
        public string Id { get; private set; }
        public int Version { get; private set; }
        public bool IsInitialized { get; private set; }

        private IList<object> PendingChanges { get; } = new List<object>();
        private readonly IEventDispatcher _dispatcher;
        protected TState State { get; private set; }
        public bool IsDirty => this.PendingChanges.Any();
        public bool IsNew => this.Version == 0;

        protected Aggregate(IEventDispatcher dispatcher = null)
        {
            this._dispatcher = dispatcher ?? new DefaultEventDispatcher<TState>(() => this.State);
        }

        public void Init(string id) => InternalInit(id, 0, null);

        private void InternalInit(string aggregateId, int aggregateVersion, TState state)
        {
            if (String.IsNullOrEmpty(aggregateId))
                throw new ArgumentNullException(nameof(aggregateId));

            if (this.Id != null)
                throw new AggregateAlreadyInitializedException(GetType(), this.Id);

            this.Id = aggregateId;
            this.State = state ?? new TState();
            this.IsInitialized = true;
            this.PendingChanges.Clear();
            this.Version = aggregateVersion;
        }

        bool IEventSourcedAggregate.TryRestore(SnapshotInfo snapshotInfo)
        {
            if (snapshotInfo == null) throw new ArgumentNullException(nameof(snapshotInfo));

            var processed = PreprocessSnapshot(snapshotInfo);

            if (processed == null || processed.IsEmpty)
                return false;

            this.InternalInit(
                processed.SourceId,
                processed.SourceVersion,
                (TState)processed.Payload
            );

            return true;
        }

        protected virtual void PostLoadingProcessing()
        {
            // entry point for custom logic on load
        }

        /// <summary>
        /// Give chance to upcast state or just drop snapshot with empty default state
        /// </summary>
        /// <param name="snapshotInfo"></param>
        /// <returns></returns>
        protected virtual SnapshotInfo PreprocessSnapshot(SnapshotInfo snapshotInfo)
        {
            return snapshotInfo;
        }

        SnapshotInfo IEventSourcedAggregate.GetSnapshot()
        {
            return new SnapshotInfo(
                this.Id,
                this.Version,
                this.State,
                this.State.GetStateVersion()
            );
        }

        void IEventSourcedAggregate.Loaded()
        {
            PostLoadingProcessing();
        }

        void IEventSourcedAggregate.Persisted(Changeset changeset)
        {
            this.Version = changeset.AggregateVersion;
            this.PendingChanges.Clear();
        }

        void IEventSourcedAggregate.ApplyChanges(Changeset changeset)
        {
            // skip if same version
            if (changeset.AggregateVersion == this.Version)
                return;

            if (changeset.AggregateVersion != this.Version + 1)
                throw new AggregateRestoreException(this.Version + 1, changeset.AggregateVersion);

            this.Version = changeset.AggregateVersion;
            foreach (var @event in changeset.Events)
            {
                this.Dispatch(@event);
            }
        }

        Changeset IEventSourcedAggregate.GetChangeSet()
        {
            return new Changeset(
                this.Version + 1,
                this.PendingChanges.ToArray()
            );
        }

        private void Dispatch(object @event)
        {
            this._dispatcher.Dispatch(@event);
        }

        protected void Emit(object @event)
        {
            this.PendingChanges.Add(@event);
            this.Dispatch(@event);
        }
    }
}