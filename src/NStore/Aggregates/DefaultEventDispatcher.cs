using System;
using System.Reflection;

namespace NStore.Aggregates
{
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
}