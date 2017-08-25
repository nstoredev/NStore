using System;
using System.Reflection;
using System.Threading.Tasks;

namespace NStore.Processing
{
    public abstract class AsyncPayloadProcessor : IAsyncPayloadProcessor, IPayloadProcessor
    {
        protected BindingFlags GetMethodFlags = BindingFlags.NonPublic | BindingFlags.Instance;

        public virtual Task ProcessAsync(object payload)
        {
            var mi = GetConsumerOf("On", payload);
            return (Task) mi?.Invoke(this, new object[] { payload });
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

        public void Process(object payload)
        {
            throw new NotImplementedException();
        }
    }
}