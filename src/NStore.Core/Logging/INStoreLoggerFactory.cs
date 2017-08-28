namespace NStore.Core.Logging
{
    public interface INStoreLoggerFactory
    {
        INStoreLogger CreateLogger(string categoryName);
    }
}