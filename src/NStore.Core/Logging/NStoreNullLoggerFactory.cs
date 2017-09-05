namespace NStore.Core.Logging
{
    public class NStoreNullLoggerFactory : INStoreLoggerFactory
    {
        public static readonly INStoreLoggerFactory Instance = new NStoreNullLoggerFactory();

        private NStoreNullLoggerFactory()
        {
            
        }

        public INStoreLogger CreateLogger(string categoryName)
        {
            return NStoreNullLogger.Instance;
        }
    }
}