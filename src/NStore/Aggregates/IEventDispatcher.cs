namespace NStore.Aggregates
{
    public interface IEventDispatcher
    {
        void Dispatch(object @event);
    }
}