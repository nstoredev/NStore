using NStore.Core.Streams;

namespace NStore.Core.Processing
{
    public static class StreamProcessorExtensions
    {
        public static StreamProcessor Fold(this IReadOnlyStream stream, IPayloadProcessor payloadProcessor = null)
        {
            var processor = new StreamProcessor(stream, payloadProcessor ?? DelegateToPrivateEventHandlers.Instance);
            return processor;
        }
    }
}