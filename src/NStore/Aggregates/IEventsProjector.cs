using System.Threading.Tasks;

namespace NStore.Aggregates
{
    public interface IEventsProjector
    {
        void Project(object @event);
    }
}