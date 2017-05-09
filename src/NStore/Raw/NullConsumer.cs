namespace NStore.Raw
{
    public class NullConsumer : IConsumer
    {
        public static readonly NullConsumer Instance = new NullConsumer();

        private NullConsumer()
        {
        }

        public ScanCallbackResult Consume(long partitionIndex, object payload)
        {
            return ScanCallbackResult.Continue;
        }
    }
}