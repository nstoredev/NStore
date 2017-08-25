using System;
using System.Threading.Tasks;
using NStore.Persistence;
using NStore.Streams;

namespace NStore.Processing
{
    public static class ProcessorExtensions
    {
        public static Task<TResult> RunAsync<TResult>(this IStream stream)
            where TResult : IPayloadProcessor, new()
        {
            var processor = new StreamProcessor<TResult>();
            return processor.RunAsync(stream);
        }

        public static Task<TResult> RunWhereAsync<TResult>(this IStream stream, Func<IChunk, bool> filter)
            where TResult : IPayloadProcessor, new()
        {
            var processor = new StreamProcessor<TResult>(filter);
            return processor.RunAsync(stream);
        }

        public static Task<TResult> RunAsync<TResult>(this IStream stream, int fromIndexInclusive)
            where TResult : IPayloadProcessor, new()
        {
            var processor = new StreamProcessor<TResult>();
            return processor.RunAsync(stream, fromIndexInclusive);
        }
    }
}