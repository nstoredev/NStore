namespace NStore.Logging
{
    public interface INStoreLoggerFactory
    {
        INStoreLogger CreateLogger(string categoryName);
    }
}