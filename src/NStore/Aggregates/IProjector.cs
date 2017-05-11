namespace NStore.Aggregates
{
    public interface IProjector
    {
        void Project(object @event);
    }
}