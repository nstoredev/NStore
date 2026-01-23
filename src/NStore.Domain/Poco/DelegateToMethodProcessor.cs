using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace NStore.Domain.Poco
{
    public class DelegateToMethodProcessor : ICommandProcessor
    {
        private static readonly ConcurrentDictionary<(Type, string), Func<object, object, object>> DelegateCache =
            new ConcurrentDictionary<(Type, string), Func<object, object, object>>();

        private readonly string _methodName;

        public DelegateToMethodProcessor() : this("Execute")
        {
        }

        public DelegateToMethodProcessor(string methodName)
        {
            _methodName = methodName;
        }

        public object RunCommand(object state, object command)
        {
            var stateType = state.GetType();
            var invoker = DelegateCache.GetOrAdd((stateType, _methodName), key => CreateDelegate(key.Item1, key.Item2));

            if (invoker == null)
            {
                throw new MissingMethodException($"Type {stateType} must implement method 'object {_methodName}(object command)");
            }

            return invoker(state, command);
        }

        private static Func<object, object, object> CreateDelegate(Type stateType, string methodName)
        {
            var mi = stateType.GetMethod(
                methodName,
                BindingFlags.Public | BindingFlags.Instance,
                null,
                new[] { typeof(object) },
                null
            );

            if (mi == null)
            {
                return null;
            }

            // Build: (object state, object cmd) => ((StateType)state).Method(cmd)
            var stateParam = Expression.Parameter(typeof(object), "state");
            var commandParam = Expression.Parameter(typeof(object), "command");

            var call = Expression.Call(
                Expression.Convert(stateParam, stateType),
                mi,
                commandParam
            );

            // Handle void return (wrap in block returning null) or object return
            Expression body;
            if (mi.ReturnType == typeof(void))
            {
                body = Expression.Block(call, Expression.Constant(null, typeof(object)));
            }
            else
            {
                body = Expression.Convert(call, typeof(object));
            }

            return Expression.Lambda<Func<object, object, object>>(body, stateParam, commandParam).Compile();
        }
    }
}