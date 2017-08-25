using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NStore.Persistence;
using NStore.Streams;

namespace NStore.Processing
{
    public class StreamProcessor<TResult> where TResult : IPayloadProcessor, new()
    {
        private class Reducer: ISubscription 
        {
            public TResult Result { get; private set; }
            private readonly Func<IChunk, bool> _filter;

            public Reducer(Func<IChunk, bool> filter)
            {
                this.Result = new TResult();
                _filter = filter;
            }

            public Task OnStartAsync(long indexOrPosition)
            {
                return Task.CompletedTask;
            }

            public async Task<bool> OnNextAsync(IChunk chunk)
            {
                if (_filter == null || _filter(chunk))
                {
                    if (this.Result is IAsyncPayloadProcessor)
                    {
                        await ((IAsyncPayloadProcessor) this.Result).ProcessAsync(chunk.Payload).ConfigureAwait(false);
                    }
                    else
                    {
                        this.Result.Process(chunk.Payload);
                    }
                }

                return true;
            }

            public Task CompletedAsync(long indexOrPosition)
            {
                return Task.CompletedTask;
            }

            public Task StoppedAsync(long indexOrPosition)
            {
                return Task.CompletedTask;
            }

            public Task OnErrorAsync(long indexOrPosition, Exception ex)
            {
                throw ex;
            }
        }

        private readonly Reducer _reducer;

        public StreamProcessor() : this(null)
        {
        }

        public StreamProcessor(Func<IChunk, bool> filter)
        {
            this._reducer = new Reducer(filter);
        }

        public async Task<TResult> RunAsync(IStream stream, long fromIndexInclusive)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            await stream.ReadAsync(this._reducer, fromIndexInclusive).ConfigureAwait(false);
            return this._reducer.Result;
        }

        public async Task<TResult> RunAsync(IStream stream)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            await stream.ReadAsync(this._reducer).ConfigureAwait(false);
            return this._reducer.Result;
        }
    }
}
