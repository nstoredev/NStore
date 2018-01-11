using System;

namespace NStore.Core.Processing
{
    public class DelegateToLambdaPayloadProcessor<TState> : IPayloadProcessor
    {
        private readonly Func<TState, object, object> _fn;

        public DelegateToLambdaPayloadProcessor(Func<TState, object, object> fn)
        {
            _fn = fn;
        }

        public object Process(object state, object payload)
        {
            return _fn((TState)state, payload);
        }
    }
}