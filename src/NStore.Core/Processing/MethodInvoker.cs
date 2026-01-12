using System;
using System.Collections.Concurrent;
using System.Reflection;

namespace NStore.Core.Processing
{
    public static class MethodInvoker
    {
        private static readonly BindingFlags NonPublic = BindingFlags.NonPublic | BindingFlags.Instance;
        private static readonly BindingFlags Public = BindingFlags.Public | BindingFlags.Instance;

        // Cache for MethodInfo lookups to avoid repeated reflection calls on hot path
        // Key: (instanceType, methodName, parameterType, isPublic)
        // Value: MethodInfo or null if method doesn't exist
        private static readonly ConcurrentDictionary<(Type, string, Type, bool), MethodInfo> MethodCache = new();

        private static MethodInfo GetCachedMethod(Type instanceType, string methodName, Type parameterType, bool isPublic)
        {
            var key = (instanceType, methodName, parameterType, isPublic);

            return MethodCache.GetOrAdd(key, k =>
            {
                var (type, name, paramType, pub) = k;
                return type.GetMethod(
                    name,
                    pub ? Public : NonPublic,
                    null,
                    new Type[] { paramType },
                    null
                );
            });
        }

        public static object CallNonPublicIfExists(this object instance, string methodName, object @parameter)
        {
            var mi = GetCachedMethod(instance.GetType(), methodName, parameter.GetType(), isPublic: false);
            return mi == null ? null : Execute(mi, instance, parameter);
        }

        public static object CallNonPublicIfExists(this object instance, string[] methodNames, object @parameter)
        {
            var instanceType = instance.GetType();
            var parameterType = parameter.GetType();

            foreach (var methodName in methodNames)
            {
                var mi = GetCachedMethod(instanceType, methodName, parameterType, isPublic: false);
                if (mi != null)
                {
                    return Execute(mi, instance, parameter);
                }
            }
            return null;
        }

        public static object CallPublicIfExists(this object instance, string methodName, object @parameter)
        {
            var mi = GetCachedMethod(instance.GetType(), methodName, parameter.GetType(), isPublic: true);
            return mi == null ? null : Execute(mi, instance, parameter);
        }

        public static object CallPublic(this object instance, string methodName, object @parameter)
        {
            var mi = GetCachedMethod(instance.GetType(), methodName, parameter.GetType(), isPublic: true);

            if (mi == null)
            {
                throw new MissingMethodException(instance.GetType().FullName, methodName);
            }

            return Execute(mi, instance, parameter);
        }

        private static object Execute(MethodInfo mi, object instance, object @parameter)
        {
            try
            {
                return mi.Invoke(instance, new object[] {parameter});
            }
            catch (TargetInvocationException e)
            {
                if (e.InnerException != null)
                {
                    throw e.InnerException;
                }
                throw;
            }
        }
    }
}