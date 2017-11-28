using System;
using System.Reflection;

namespace NStore.Domain.Poco
{
    public class DelegateToMethodProcessor : ICommandProcessor
    {
        private MethodInfo _execute;
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
            if (_execute == null)
            {
                _execute = state.GetType().GetMethod(
                    _methodName,
                    BindingFlags.Public | BindingFlags.Instance,
                    null,
                    new[] { typeof(object) },
                    null
                );

                if (_execute == null)
                {
                    throw new MissingMethodException($"Type {state.GetType()} must implement method 'object {_methodName}(object command)");
                }
            }

            try
            {
                return _execute.Invoke(state, new[] {command});
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