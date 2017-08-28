namespace NStore.Processing
{
    public static class PayloadProcessorExtensions
    {
        public static void FoldEach(this IPayloadProcessor processor, object state, object[] payloads)
        {
            foreach (var payload in payloads)
            {
                processor.Process(state, payload);
            }
        }
    }
}