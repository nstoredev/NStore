using System;
using System.Reflection;

namespace NStore.Core.Processing
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
}