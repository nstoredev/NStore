using System;

namespace NStore.Domain.Poco
{
    public class DelegateProcessor<TState> : ICommandProcessor
    {
        private readonly Func<TState, object, object> _fn;

        public DelegateProcessor(Func<TState, object, object> fn)
        {
            _fn = fn;
        }

        public object RunCommand(object state, object command)
        {
            return _fn((TState)state, command);
        }
    }
}