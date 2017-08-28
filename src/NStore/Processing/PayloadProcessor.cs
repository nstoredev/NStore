using System;
using System.Reflection;

namespace NStore.Processing
{
    public static class MethodInvoker
    {
        static BindingFlags NonPublic = BindingFlags.NonPublic | BindingFlags.Instance;
        static BindingFlags Public = BindingFlags.Public | BindingFlags.Instance;

        public static object CallNonPublicIfExists(this object instance, string methodName, object @parameter)
        {
            var mi = instance.GetType().GetMethod(
                methodName,
                NonPublic,
                null,
                new Type[] { @parameter.GetType() },
                null
            );

            return mi?.Invoke(instance, new object[] { parameter });
        }

        public static object CallPublicIfExists(this object instance, string methodName, object @parameter)
        {
            var mi = instance.GetType().GetMethod(
                methodName,
                Public,
                null,
                new Type[] { @parameter.GetType() },
                null
            );

            return mi?.Invoke(instance, new object[] { parameter });
        }
    }

    public sealed class DelegateToPrivateEventHandlers : IPayloadProcessor
    {
        public static readonly IPayloadProcessor Instance = new DelegateToPrivateEventHandlers();

        private DelegateToPrivateEventHandlers()
        {
        }

        public object Process(object state, object payload)
        {
            return state.CallNonPublicIfExists("On", payload);
        }
    }

    public sealed class DelegateToPublicEventHandlers : IPayloadProcessor
    {
        public static readonly IPayloadProcessor Instance = new DelegateToPublicEventHandlers();

        private DelegateToPublicEventHandlers()
        {
        }

        public object Process(object state, object payload)
        {
            return state.CallPublicIfExists("On", payload);
        }
    }

    public static class PayloadProcessorExtensions
    {
        public static void FoldEach(this IPayloadProcessor processor, object state, object[] payloads)
        {
            foreach (var payload in payloads)
            {
                processor.Process(state, payload);
            }
        }
    }
}