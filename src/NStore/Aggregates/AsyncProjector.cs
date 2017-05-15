using System;
using System.Reflection;
using System.Threading.Tasks;

namespace NStore.Aggregates
{
    public abstract class AsyncProjector : IAsyncProjector
    {
        public virtual async Task Project(object @event)
        {
            var mi = GetConsumerOf("On", @event);
            var invoke = (Task) mi?.Invoke(this, new object[] {@event});
            if (invoke != null)
                await invoke;
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