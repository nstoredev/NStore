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

    public delegate object Executor(object command);

    public class StateRouter
    {
        private readonly IDictionary<string, Executor> _nodes = new Dictionary<string, Executor>();
        private Executor _state;

        public void TransitionTo(string node)
        {
            _state = _nodes[node];
        }

        public object Execute(object command)
        {
            return _state(command);
        }

        public StateRouter Define(string node, Executor executor)
        {
            _nodes[node] = executor;
            return this;
        }

        public StateRouter Start(string node)
        {
            _state = _nodes[node];
            return this;
        }
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

    public class OnCommandProcessor : ICommandProcessor
    {
        private readonly MethodInfo _onCommand;

        public OnCommandProcessor(MethodInfo onCommand)
        {
            _onCommand = onCommand;
        }

        public object RunCommand(object state, object command)
        {
            try
            {
                return _onCommand.Invoke(state, new[] {command});
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
        private ICommandProcessor _processor = DefaultCommandProcessor.Instance;

        protected override void AfterInit()
        {
            base.AfterInit();

            var onCommand = typeof(TState).GetMethod(
                "OnCommand",
                BindingFlags.Public | BindingFlags.Instance,
                null,
                new[] {typeof(object)},
                null
            );

            if (onCommand != null)
            {
                _processor = new OnCommandProcessor(onCommand);
            }
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