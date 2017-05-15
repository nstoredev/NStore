using System.Threading.Tasks;

namespace NStore.Aggregates
{
    public interface IProjector
    {
        void Project(object @event);
    }

    public interface IAsyncProjector
    {
        Task Project(object @event);
    }
}