using System.Xml.Linq;

namespace NStore.Aggregates
{
    public abstract class Aggregate : IAggregate
    {
        public int Version { get; private set; }
        public bool Initialized => Version > 0;

        public void Append(object @event)
        {
            this.Version++;
        }
    }
}