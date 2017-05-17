using System;
using System.Reflection;
using System.Threading.Tasks;
using NStore.Aggregates;

namespace NStore.Sample.Projections
{
    public interface IProjection
    {
        Task Project(Changeset changes);
    }

    public abstract class AbstractProjection : IProjection
    {
        public virtual async Task Project(Changeset changes)
        {
            foreach (var @event in changes.Events)
            {
                var mi = GetConsumerOf("On", @event);
                var invoke = (Task)mi?.Invoke(this, new object[] { @event });
                if (invoke != null)
                    await invoke;
            }
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