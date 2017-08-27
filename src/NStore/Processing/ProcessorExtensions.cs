using System;
using System.Threading.Tasks;
using NStore.Persistence;
using NStore.Streams;

namespace NStore.Processing
{
    public static class ProcessorExtensions
    {
        public static StreamProcessor<TResult> Fold<TResult>(this IStream stream)
            where TResult : IPayloadProcessor, new()
        {
            var processor = new StreamProcessor<TResult>(stream);
            return processor;
        }
    }
}