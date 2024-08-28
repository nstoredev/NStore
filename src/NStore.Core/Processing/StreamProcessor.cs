﻿using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using NStore.Core.Streams;

namespace NStore.Core.Processing
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

            public Reducer(TResult state, Func<long, long, bool> onMissing, IPayloadProcessor payloadProcessor)
            {
                _state = state;
                _onMissing = onMissing;
                _processor = payloadProcessor;
            }

            public Task OnStartAsync(long indexOrPosition, CancellationToken cancellationToken)
            {
                _nextExpectedIndex = indexOrPosition;
                return Task.CompletedTask;
            }

            public async Task<bool> OnNextAsync(IChunk chunk, CancellationToken cancellationToken)
            {
                if (chunk.Index != _nextExpectedIndex
                    && _onMissing != null
                    && !_onMissing(_nextExpectedIndex, chunk.Index - 1)
                )
                {
                    return false;
                }

                var result = this._processor.Process(_state, chunk.Payload);

                if (result is Task task)
                {
                    await task.ConfigureAwait(false);
                }

                _nextExpectedIndex = chunk.Index + 1;

                return true;
            }

            public Task CompletedAsync(long indexOrPosition, CancellationToken cancellationToken)
            {
                LastIndex = indexOrPosition;
                return Task.CompletedTask;
            }

            public Task StoppedAsync(long indexOrPosition, CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }

            public Task OnErrorAsync(long indexOrPosition, Exception ex, CancellationToken cancellationToken)
            {
                throw ex;
            }
        }

        private readonly IReadOnlyStream _source;
        private ISnapshotStore _snapshots;
        private long? _upToIndex;
        private Func<long, long, bool> _onMissing;

        public StreamProcessor(IReadOnlyStream source)
        {
            _source = source ?? throw new ArgumentNullException(nameof(source));
        }

        public Task<TResult> RunAsync<TResult>() where TResult : new()
        {
            return RunAsync<TResult>(DelegateToPrivateEventHandlers.Instance, default(CancellationToken));
        }

        public Task<TResult> RunAsync<TResult>(Func<TResult, object, object> processor) where TResult : new()
        {
            return RunAsync<TResult>(
                new DelegateToLambdaPayloadProcessor<TResult>(processor),
                default(CancellationToken)
            );
        }

        public Task<TResult> RunAsync<TResult>(Func<TResult, object, Task<object>> processor) where TResult : new()
        {
            return RunAsync<TResult>(
                new DelegateToLambdaPayloadProcessor<TResult>(processor),
                default(CancellationToken)
            );
        }

        public async Task<TResult> RunAsync<TResult>(IPayloadProcessor processor, CancellationToken cancellationToken)
            where TResult : new()
        {
            long startIndex = 1;
            TResult state = default(TResult);
            string snapshotId = this._source.Id + "/" + typeof(TResult).Name;

            if (_snapshots != null)
            {
                var si = await LoadSnapshot(snapshotId, cancellationToken).ConfigureAwait(false);

                if (si != null)
                {
                    state = (TResult) si.Payload;
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

            var reducer = new Reducer<TResult>(state, _onMissing, processor);
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
