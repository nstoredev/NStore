using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Logging;

namespace NStore.Core.Persistence
{
    public class PollingClient
    {
        private class Sequencer : ISubscription
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

                RetriesOnHole = 0;
                _stopOnHole = true;
                Position = chunk.Position;
                Processed++;
                return _subscription.OnNextAsync(chunk);
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

        [Obsolete("Use IsActive")]
        public bool IsPolling => IsActive;
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
            _source.Cancel();
            _source = null;

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

        public Task Poll(int timeoutInMilliseconds)
        {
            var timeout = new CancellationTokenSource(timeoutInMilliseconds);
            return Poll(timeout.Token);
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