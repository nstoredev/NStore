using System.Collections;
using NStore.Core.Processing;

// ReSharper disable ClassNeverInstantiated.Global

namespace NStore.Domain
{
    public class PocoAggregate<TState> : Aggregate<TState> where TState : class, new()
    {
        public void Do(object command)
        {
            var events = State.CallPublic("Do", command);
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