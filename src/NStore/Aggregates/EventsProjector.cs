using System;
using System.Reflection;

namespace NStore.Aggregates
{
    public abstract class EventsProjector : IEventsProjector
    {
        protected BindingFlags GetMethodFlags = BindingFlags.NonPublic | BindingFlags.Instance;

        public virtual void Project(object @event)
        {
            var mi = GetConsumerOf("On", @event);
            mi?.Invoke(this, new object[] { @event });
        }

        private MethodInfo GetConsumerOf(string methodName, object @event)
        {
            var mi = GetType().GetMethod(
                methodName,
                GetMethodFlags,
                null,
                new Type[] { @event.GetType() },
                null
            );
            return mi;
        }
    }
}