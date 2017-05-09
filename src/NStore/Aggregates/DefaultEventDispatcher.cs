using System;
using System.Reflection;

namespace NStore.Aggregates
{
    public class DefaultEventDispatcher<TState> : IEventDispatcher where TState : IProjector
    {
        private readonly Func<TState> _getState;

        public DefaultEventDispatcher(Func<TState> getState)
        {
            _getState = getState;
        }

        public void Dispatch(object @event)
        {
            var state = _getState();
            state.Project(@event);
        }
    }
}