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
        private class Reducer<TResult> : ISubscription
        {
            private readonly TResult _state;
            private readonly Func<long, long, bool> _onMissing;
            public long LastIndex { get; private set; }
            private readonly IPayloadProcessor _processor;
            private long _nextExpectedIndex = 0;
            public Reducer(TResult state, Func<long, long, bool> onMissing)
            {
                _state = state;
                _onMissing = onMissing;
                _processor = DelegateToPrivateEventHandlers.Instance;
            }

            public Task OnStartAsync(long indexOrPosition)
            {
                _nextExpectedIndex = indexOrPosition;
                return Task.CompletedTask;
            }

            public async Task<bool> OnNextAsync(IChunk chunk)
            {
                if (chunk.Index != _nextExpectedIndex && _onMissing != null)
                {
                    if (!_onMissing(_nextExpectedIndex, chunk.Index - 1))
                    {
                        return false;
                    }
                }

                var result = this._processor.Process(_state, chunk.Payload);

                if (result is Task task)
                {
                    await task.ConfigureAwait(false);
                }

                _nextExpectedIndex = chunk.Index + 1;

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
        private Func<long, long, bool> _onMissing;

        public StreamProcessor(IStream source)
        {
            _source = source ?? throw new ArgumentNullException(nameof(source));
        }

        public Task<TResult> RunAsync<TResult>() where TResult : new()
        {
            return RunAsync<TResult>(default(CancellationToken));
        }

        public async Task<TResult> RunAsync<TResult>(CancellationToken cancellationToken) where TResult : new()
        {
            long startIndex = 1;
            TResult state = default(TResult);
            string snapshotId = this._source.Id + "/" + typeof(TResult).Name;

            if (_snapshots != null)
            {
                var si = await LoadSnapshot(snapshotId, cancellationToken);

                if (si != null)
                {
                    state = (TResult)si.Payload;
                    if (_upToIndex.HasValue && si.SourceVersion == _upToIndex.Value)
                    {
                        return state;
                    }

                    startIndex = si.SourceVersion + 1;
                }
            }

            if (state == null)
            {
                state = new TResult();
            }

            var reducer = new Reducer<TResult>(state, _onMissing);
            await _source.ReadAsync(reducer, startIndex, _upToIndex ?? long.MaxValue, cancellationToken)
                .ConfigureAwait(false);

            if (_snapshots != null && reducer.LastIndex > 0)
            {
                await _snapshots.AddAsync(snapshotId, new SnapshotInfo(
                    _source.Id,
                    reducer.LastIndex,
                    state,
                    "1"
                )).ConfigureAwait(false);
            }

            return state;
        }

        private async Task<SnapshotInfo> LoadSnapshot(string snapshotId, CancellationToken cancellationToken)
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
            return si;
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

        public StreamProcessor OnMissing(Func<long, long, bool> action)
        {
            this._onMissing = action;
            return this;
        }
    }
}