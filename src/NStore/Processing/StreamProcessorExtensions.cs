using System;
using System.Threading.Tasks;
using NStore.Persistence;
using NStore.Streams;

namespace NStore.Processing
{
    public static class StreamProcessorExtensions
    {
        public static StreamProcessor Fold(this IStream stream)
        {
            var processor = new StreamProcessor(stream);
            return processor;
        }
    }
}