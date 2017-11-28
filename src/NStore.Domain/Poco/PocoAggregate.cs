using System;
using System.Collections;

namespace NStore.Domain.Poco
{
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

        public PocoAggregate(Func<TState, object, object> executor)
        {
            _processor = new DelegateProcessor<TState>(executor);
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