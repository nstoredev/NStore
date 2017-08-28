namespace NStore.Domain
{
    public interface IHeadersAccessor
    {
        IHeadersAccessor Add(string key, object value);
    }
}