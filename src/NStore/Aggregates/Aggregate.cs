using System;
using System.Collections.Generic;
using System.Reflection;
using System.Xml.Linq;

namespace NStore.Aggregates
{
    public interface IEventDispatcher
    {
        void Dispatch(object @event);
    }

    public class DefaultEventDispatcher<TState> : IEventDispatcher where TState : AggregateState
    {
        private readonly Func<object> _getState;

        public DefaultEventDispatcher(Func<object> getState)
        {
            _getState = getState;
        }

        public void Dispatch(object @event)
        {
            var state = _getState();
            var mi = GetMethod(state, @event);
            mi?.Invoke(state, new object[] {@event});
        }

        private MethodInfo GetMethod(object target, object @event)
        {
            //@@TODO: cache
            var type = target.GetType();

            //@@TODO: access private methods
            var mi = type.GetTypeInfo()
                .GetMethod(
                    "On",
                    new Type[] {@event.GetType()}
                );

            return mi;
        }
    }

    public abstract class AggregateState
    {
    }

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