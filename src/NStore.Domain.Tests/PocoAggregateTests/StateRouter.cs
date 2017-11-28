using System;
using System.Collections.Generic;

namespace NStore.Domain.Tests.PocoAggregateTests
{
    internal class StateRouter
    {
        public delegate object Executor(object command);

        private readonly IDictionary<string, Executor> _nodes = new Dictionary<string, Executor>();
        private Executor _state;
        private string _current;

        public void TransitionTo(string node)
        {
            _current = node;
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

        public object Unhandled(object command)
        {
            throw new InvalidOperationException($"Command {command.GetType().FullName} was not handled by state {_current}");
        }
    }
}