using System;
using System.Reflection;

namespace NStore.Processing
{
    public static class MethodInvoker
    {
        static BindingFlags NonPublic = BindingFlags.NonPublic | BindingFlags.Instance;
        static BindingFlags Public = BindingFlags.Public | BindingFlags.Instance;

        public static void CallNonPublicIfExists(this object instance, string methodName, object @parameter)
        {
            var mi = instance.GetType().GetMethod(
                methodName,
                NonPublic,
                null,
                new Type[] { @parameter.GetType() },
                null
            );

            mi?.Invoke(instance, new object[] {parameter});
        }
        
        public static void CallPublicIfExists(this object instance, string methodName, object @parameter)
        {
            var mi = instance.GetType().GetMethod(
                methodName,
                Public,
                null,
                new Type[] { @parameter.GetType() },
                null
            );

            mi?.Invoke(instance, new object[] {parameter});
        }
    }

    public class DelegateToPrivateEventHandlers : IPayloadProcessor
    {
        private readonly Object _instance;

        public DelegateToPrivateEventHandlers(Object instance)
        {
            _instance = instance;
        }

        public void Process(object payload)
        {
            _instance.CallNonPublicIfExists("On",payload);
        }
    }
    
    public class DelegateToPublicEventHandlers : IPayloadProcessor
    {
        private readonly Object _instance;

        public DelegateToPublicEventHandlers(Object instance)
        {
            _instance = instance;
        }

        public void Process(object payload)
        {
            _instance.CallPublicIfExists("On",payload);
        }
    }
}