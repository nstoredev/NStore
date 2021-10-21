using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace NStore.Core.Processing
{
    public static class FastMethodInvoker
    {
        static BindingFlags NonPublic = BindingFlags.NonPublic | BindingFlags.Instance;
        static BindingFlags Public = BindingFlags.Public | BindingFlags.Instance;

        public static object CallDynamically(object instance, string methodName, object @parameter)
        {
            var paramType = @parameter.GetType();
            var cacheKey = $"{instance.GetType().FullName}/{methodName}/{paramType.FullName}";
            if (!lcgCacheFunction.TryGetValue(cacheKey, out var caller))
            {
                var mi = instance.GetType().GetMethod(
                    methodName,
                    Public | NonPublic,
                    null,
                    new Type[] { paramType },
                    null
                );

                if (mi != null)
                {
                    if (mi.ReturnType == typeof(void))
                    {
                        caller = ReflectAction(instance.GetType(), mi);
                    }
                    else
                    {
                        caller = ReflectFunction(instance.GetType(), mi);
                    }
                }
                else
                {
                    caller = null;
                }

                lcgCacheFunction[cacheKey] = caller;
            }
            if (caller != null)
            {
                return caller(instance, @parameter);
            }

            return null;
        }

        /// <summary>
        /// Cache of appliers, for each domain object I have a dictionary of actions
        /// </summary>
        private static ConcurrentDictionary<string, Func<Object, Object, Object>> lcgCacheFunction = new ConcurrentDictionary<string, Func<Object, Object, Object>>();

        public static Func<Object, Object, Object> ReflectAction(Type objType, MethodInfo methodinfo)
        {
            DynamicMethod retmethod = new DynamicMethod(
                "Invoker" + methodinfo.Name,
                (Type)null,
                new Type[] { typeof(Object), typeof(Object) },
                objType,
                true); //methodinfo.GetParameters().Single().ParameterType
            ILGenerator ilgen = retmethod.GetILGenerator();
            ilgen.Emit(OpCodes.Ldarg_0);
            ilgen.Emit(OpCodes.Castclass, objType);
            ilgen.Emit(OpCodes.Ldarg_1);
            ilgen.Emit(OpCodes.Callvirt, methodinfo);
            ilgen.Emit(OpCodes.Ret);

            //To have similar functionalities we simply wrap action invocation in another function that
            //simply return null. Caller can simply ignore the return value.
            var action = (Action<Object, Object>)retmethod.CreateDelegate(typeof(Action<Object, Object>));
            return (a, b) => { action(a, b); return null; };
        }

        public static Func<Object, Object, Object> ReflectFunction(Type objType, MethodInfo methodinfo)
        {
            DynamicMethod retmethod = new DynamicMethod(
                "Invoker" + methodinfo.Name,
                typeof(Object),
                new Type[] { typeof(Object), typeof(Object) },
                objType,
                true); //methodinfo.GetParameters().Single().ParameterType
            ILGenerator ilgen = retmethod.GetILGenerator();
            ilgen.Emit(OpCodes.Ldarg_0);
            ilgen.Emit(OpCodes.Castclass, objType);
            ilgen.Emit(OpCodes.Ldarg_1);
            ilgen.Emit(OpCodes.Callvirt, methodinfo);
            ilgen.Emit(OpCodes.Ret);
            return (Func<Object, Object, Object>)retmethod.CreateDelegate(typeof(Func<Object, Object, Object>));
        }
    }
}