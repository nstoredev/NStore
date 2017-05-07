using System.Collections.Generic;
using System.Xml.Linq;

namespace NStore.Aggregates
{
    public abstract class Aggregate : IAggregate
    {
        public int Version { get; private set; }
        public bool IsInitialized => Version > 0;
        public IList<object> UncommittedEvents { get; private set; } = new List<object>();

        public void Append(object @event)
        {
            this.Version++;
        }

        protected void Raise(object @event)
        {
            this.Version++;
            this.UncommittedEvents.Add(@event);
        }
    }
}