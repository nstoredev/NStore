using System;
using System.Reflection;

namespace NStore.Aggregates
{
    public abstract class SyncProjector : IProjector
    {
        public virtual void Project(object @event)
        {
            var mi = GetConsumerOf("On", @event);
            mi?.Invoke(this, new object[] { @event });
        }

        private MethodInfo GetConsumerOf(string methodName, object @event)
        {
            //@@TODO: access private methods + cache
            var mi = GetType().GetTypeInfo()
                .GetMethod(
                    methodName,
                    new Type[] { @event.GetType() }
                );

            return mi;
        }
    }
}