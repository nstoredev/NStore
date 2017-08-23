namespace NStore.Logging
{
    public class NStoreNullLoggerFactory : INStoreLoggerFactory
    {
        public INStoreLogger CreateLogger(string categoryName)
        {
            return NStoreNullLogger.Instance;
        }
    }
}