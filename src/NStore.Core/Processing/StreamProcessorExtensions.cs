using NStore.Core.Streams;

namespace NStore.Core.Processing
{
    public static class StreamProcessorExtensions
    {
        public static StreamProcessor Fold(this IReadOnlyStream stream)
        {
            var processor = new StreamProcessor(stream, DelegateToPrivateEventHandlers.Instance);
            return processor;
        }
        public static StreamProcessor Fold(this IReadOnlyStream stream, IPayloadProcessor payloadProcessor)
        {
            var processor = new StreamProcessor(stream, payloadProcessor ?? DelegateToPrivateEventHandlers.Instance);
            return processor;
        }
    }
}