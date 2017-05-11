namespace NStore.Aggregates
{
    public interface IHeadersAccessor
    {
        IHeadersAccessor Add(string key, object value);
    }
}