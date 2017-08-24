using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NStore.Aggregates;
using NStore.Persistence;
using NStore.Streams;

namespace NStore.Processing
{
    public class StreamProcessor<TResult> : ISubscription where TResult : IPayloadProcessor, new()
    {
        private readonly Func<IChunk, bool> _include;
        public TResult Result { get; private set; }

        public StreamProcessor() : this(null)
        {
        }

        public StreamProcessor(Func<IChunk, bool> include)
        {
            _include = include;
            Result = new TResult();
        }

        public Task OnStartAsync(long position)
        {
            return Task.CompletedTask;
        }

        public Task<bool> OnNextAsync(IChunk data)
        {
            if (_include == null || _include(data))
            {
                this.Result.Process(data.Payload);
            }

            return Task.FromResult(true);
        }

        public Task CompletedAsync(long position)
        {
            return Task.CompletedTask;
        }

        public Task StoppedAsync(long position)
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(long position, Exception ex)
        {
            throw ex;
        }

        public async Task<TResult> RunAsync(IStream stream, long fromIndexInclusive)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            await stream.ReadAsync(this, fromIndexInclusive).ConfigureAwait(false);
            return Result;
        }

        public async Task<TResult> RunAsync(IStream stream)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            await stream.ReadAsync(this).ConfigureAwait(false);
            return Result;
        }
    }

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
