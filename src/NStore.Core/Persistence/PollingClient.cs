using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Logging;

namespace NStore.Core.Persistence
{
    public class PollingClient
    {
        private sealed class Sequencer : ISubscription
        {
            private readonly ISubscription _subscription;
            private readonly INStoreLogger _logger;
            public long Position { get; private set; } = 0;

            public long Processed { get; private set; }
            public int RetriesOnHole { get; private set; }
            private bool _stopOnHole = true;
            public Sequencer(long lastPosition, ISubscription subscription, INStoreLogger logger)
            {
                Position = lastPosition;
                _subscription = subscription;
                _logger = logger;
            }

            public Task<bool> OnNextAsync(IChunk chunk)
            {
                _logger.LogDebug("OnNext {Position}", chunk.Position);

                if (chunk.Position != Position + 1)
                {
                    if (_stopOnHole)
                    {
                        RetriesOnHole++;
                        _logger.LogDebug("Hole detected on {Position} - {Retries}", chunk.Position, RetriesOnHole);
                        return Task.FromResult(false);
                    }
                    _logger.LogWarning("Skipping hole on {Position}", chunk.Position);
                }

                // At-least-once delivery: dispatch the chunk to the consumer FIRST and only
                // advance our position once it completes successfully. If OnNextAsync faults
                // or is cancelled we deliberately leave Position untouched so the next poll
                // re-reads (redelivers) this same chunk instead of skipping it.
                var processing = _subscription.OnNextAsync(chunk);
                if (processing.Status == TaskStatus.RanToCompletion)
                {
                    // Fast path: the consumer completed synchronously (e.g. a cached
                    // Subscription.Continue/Stop task). Acknowledge inline and reuse the cached
                    // result task to keep this common path allocation-free.
                    return Acknowledge(chunk, processing.Result);
                }

                // The consumer is running asynchronously, or already faulted/cancelled. Await it
                // so that acknowledgement happens only after a successful completion; a faulted
                // or cancelled task rethrows here before MarkProcessed runs.
                return AwaitAndAcknowledge(processing, chunk);
            }

            private Task<bool> Acknowledge(IChunk chunk, bool shouldContinue)
            {
                MarkProcessed(chunk);
                return shouldContinue ? Subscription.Continue : Subscription.Stop;
            }

            // Commits the chunk as processed: advances the position (so it will not be
            // redelivered) and resets hole-detection state. Called only after the consumer has
            // successfully handled the chunk.
            private void MarkProcessed(IChunk chunk)
            {
                RetriesOnHole = 0;
                _stopOnHole = true;
                Position = chunk.Position;
                Processed++;
            }

            private async Task<bool> AwaitAndAcknowledge(Task<bool> processing, IChunk chunk)
            {
                // If the consumer faults or cancels, the await rethrows and MarkProcessed is
                // skipped, so Position stays on the previous chunk and delivery is retried.
                var shouldContinue = await processing.ConfigureAwait(false);
                MarkProcessed(chunk);
                return shouldContinue;
            }

            public Task OnStartAsync(long indexOrPosition)
            {
                _logger.LogDebug("OnStart({Position})", indexOrPosition);

                Position = indexOrPosition - 1;
                Processed = 0;
                _stopOnHole = RetriesOnHole < 5;
                return _subscription.OnStartAsync(indexOrPosition);
            }

            public Task CompletedAsync(long indexOrPosition)
            {
                _logger.LogDebug("Completed({Position})", indexOrPosition);

                if (Processed > 0)
                {
                    Position = indexOrPosition;
                }

                return _subscription.CompletedAsync(indexOrPosition);
            }

            public Task StoppedAsync(long indexOrPosition)
            {
                _logger.LogDebug("Stopped({Position})", indexOrPosition);
                return _subscription.StoppedAsync(indexOrPosition);
            }

            public Task OnErrorAsync(long indexOrPosition, Exception ex)
            {
                return _subscription.OnErrorAsync(indexOrPosition, ex);
            }
        }

        private CancellationTokenSource _source;
        private readonly IPersistence _store;
        public int PollingIntervalMilliseconds { get; set; }
        public int HoleDetectionTimeout { get; set; }
        public long Position => _sequencer.Position;

        private readonly Sequencer _sequencer;
        private readonly INStoreLogger _logger;
        private int _isPolling = 0;
        private bool _stopped = false;
        
        public bool IsActive => !_stopped;

        public PollingClient(IPersistence store, long lastPosition, ISubscription subscription, INStoreLoggerFactory inStoreLoggerFactory)
        {
            this._logger = inStoreLoggerFactory.CreateLogger(GetType().FullName);

            _sequencer = new Sequencer(lastPosition, subscription, _logger);
            _store = store;
            PollingIntervalMilliseconds = 200;
            HoleDetectionTimeout = 2000;
        }

        public async Task Stop()
        {
            if (_source != null)
            {
#if NET8_0_OR_GREATER
                await _source.CancelAsync();
#else
                _source.Cancel();       
#endif
                _source.Dispose();
                _source = null;
            }

            while (!_stopped)
            {
                await Task.Delay(100).ConfigureAwait(false);
            }
        }

        public void Start()
        {
            _source = new CancellationTokenSource();
            var token = _source.Token;
            _stopped = false;
            Task.Run(async () =>
            {
                try
                {
                    while (!token.IsCancellationRequested)
                    {
                        await Poll(token).ConfigureAwait(false);
                        if (!token.IsCancellationRequested)
                        {
                            await Task.Delay(PollingIntervalMilliseconds, token).ConfigureAwait(false);
                        }
                    }
                }
                finally
                {
                    _stopped = true;
                }
            }, token)
            .ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    var ex = t.Exception.Flatten().InnerException;
                    _logger.LogError($"Error during Poll, first exception: {ex.Message}.\n{ex}");
                }
            }, token)
            .ConfigureAwait(false);
        }

        public async Task Poll(int timeoutInMilliseconds)
        {
            using var timeout = new CancellationTokenSource(timeoutInMilliseconds);
            await Poll(timeout.Token).ConfigureAwait(false);
        }

        public Task Poll()
        {
            return Poll(CancellationToken.None);
        }

        public async Task Poll(CancellationToken token)
        {
            if (Interlocked.CompareExchange(ref _isPolling, 1, 0) == 0)
            {
                try
                {
                    await InnerPolling(token).ConfigureAwait(false);
                }
                finally
                {
                    Interlocked.Exchange(ref _isPolling, 0);
                }
            }
#if NSTORE_CHECKED_POLLER
            else
            {
                throw new PollingException("Already polling");
            }
#endif
        }

        private async Task InnerPolling(CancellationToken token)
        {
            do
            {
                using (_logger.BeginScope("Poll"))
                {
                    var start = Position + 1;
                    await _store.ReadAllAsync(
                        start,
                        _sequencer,
                        int.MaxValue,
                        token
                    ).ConfigureAwait(false);

                    if (_sequencer.RetriesOnHole > 0)
                    {
                        await Task.Delay(HoleDetectionTimeout, token).ConfigureAwait(false);
                    }

                    if (_logger.IsDebugEnabled)
                    {
                        _logger.LogDebug($"HoleDetected : {_sequencer.RetriesOnHole}");
                        _logger.LogDebug(
                            $"Polled from {start} to {_sequencer.Position}, processed {_sequencer.Processed} chunks");
                    }
                }
            } while (_sequencer.RetriesOnHole > 0);
        }
    }
}
