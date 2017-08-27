using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;
using NStore.Snapshots;
using NStore.Streams;

namespace NStore.Processing
{
    public class StreamProcessor
    {
        private class Reducer<TResult> : ISubscription where TResult : IPayloadProcessor
        {
            private readonly TResult _state;
            public long LastIndex { get; private set; }

            public Reducer(TResult state)
            {
                _state = state;
            }

            public Task OnStartAsync(long indexOrPosition)
            {
                return Task.CompletedTask;
            }

            public async Task<bool> OnNextAsync(IChunk chunk)
            {
                if (this._state is IAsyncPayloadProcessor)
                {
                    await ((IAsyncPayloadProcessor) this._state).ProcessAsync(chunk.Payload).ConfigureAwait(false);
                }
                else
                {
                    this._state.Process(chunk.Payload);
                }

                return true;
            }

            public Task CompletedAsync(long indexOrPosition)
            {
                LastIndex = indexOrPosition;
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

        private readonly IStream _source;
        private ISnapshotStore _snapshots;
        private long? _upToIndex;

        public StreamProcessor(IStream source)
        {
            _source = source ?? throw new ArgumentNullException(nameof(source));
        }

        public Task<TResult> RunAsync<TResult>() where TResult : IPayloadProcessor, new()
        {
            return RunAsync<TResult>(default(CancellationToken));
        }

        public async Task<TResult> RunAsync<TResult>(CancellationToken cancellationToken) where TResult : IPayloadProcessor, new()
        {
            long startIndex = 0;
            TResult state = default(TResult);
            string snapshotId = this._source.Id + "/" + typeof(TResult).Name;
            
            if (_snapshots != null)
            {
                SnapshotInfo si = null;
                if (_upToIndex.HasValue)
                {
                    si = await _snapshots.GetAsync(snapshotId, _upToIndex.Value, cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    si = await _snapshots.GetLastAsync(snapshotId, cancellationToken)
                        .ConfigureAwait(false);
                }

                if (si != null)
                {
                    state = (TResult) si.Payload;
                    if (_upToIndex.HasValue && si.SourceVersion == _upToIndex.Value)
                    {
                        return state;
                    }
                    
                    startIndex = si.SourceVersion;
                }
            }

            if (state == null)
            {
                state = new TResult();
            }

            var reducer = new Reducer<TResult>(state);
            await _source.ReadAsync(reducer, startIndex, _upToIndex ?? long.MaxValue, cancellationToken)
                .ConfigureAwait(false);

            if (_snapshots != null && reducer.LastIndex > 0)
            {
                await _snapshots.AddAsync(snapshotId, new SnapshotInfo(
                    _source.Id,
                    reducer.LastIndex,
                    state,
                    1
                )).ConfigureAwait(false);

            }

            return state;
        }

        public StreamProcessor WithCache(ISnapshotStore snapshots)
        {
            this._snapshots = snapshots;
            return this;
        }

        public StreamProcessor ToIndex(int upToIndex)
        {
            this._upToIndex = upToIndex;
            return this;
        }
    }
}