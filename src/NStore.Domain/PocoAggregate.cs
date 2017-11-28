using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using NStore.Core.Processing;

// ReSharper disable ClassNeverInstantiated.Global

namespace NStore.Domain
{
    public interface IPocoAggregate
    {
        void Do(object command);
    }

    public interface ICommandProcessor
    {
        object RunCommand(object state, object command);
    }

    public class DefaultCommandProcessor : ICommandProcessor
    {
        public static readonly ICommandProcessor Instance = new DefaultCommandProcessor();

        private DefaultCommandProcessor()
        {
        }

        public object RunCommand(object state, object command)
        {
            return state.CallPublic("Do", command);
        }
    }

    public class ExecuteProcessor : ICommandProcessor
    {
        private MethodInfo _execute;
        private readonly string _methodName;

        public ExecuteProcessor() : this("Execute")
        {
            
        }

        public ExecuteProcessor(string methodName)
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

    public class PocoAggregate<TState> : Aggregate<TState>, IPocoAggregate where TState : class, new()
    {
        private readonly ICommandProcessor _processor = DefaultCommandProcessor.Instance;

        public PocoAggregate()
        {
            
        }
        public PocoAggregate(ICommandProcessor processor)
        {
            _processor = processor;
        }

        public void Do(object command)
        {
            var events = _processor.RunCommand(State, command);

            if (events is IEnumerable enumerable)
            {
                foreach (var e in enumerable)
                {
                    Emit(e);
                }
                return;
            }

            if (events != null)
            {
                Emit(events);
            }
        }
    }
}