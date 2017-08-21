using System;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NStore.Persistence
{
    public class PollingException : Exception
    {
        public PollingException(string message) : base(message)
        {
        }
    }

    public class PollingClient
    {
        private class Sequencer : ISubscription
        {
            private readonly ISubscription _subscription;
            private readonly ILogger _logger;
            public long Position { get; private set; } = 0;

            public long Processed { get; private set; }
            public int RetriesOnHole { get; private set; }
            private bool _stopOnHole = true;
            public Sequencer(long lastPosition, ISubscription subscription, ILogger logger)
            {
                Position = lastPosition;
                _subscription = subscription;
                _logger = logger;
            }

            public Task<bool> OnNext(IChunk data)
            {
                _logger.LogDebug("OnNext {Position}", data.Position);

                if (data.Position != Position + 1)
                {
                    if (_stopOnHole)
                    {
                        RetriesOnHole++;
                        _logger.LogDebug("Hole detected on {Position} - {Retries}", data.Position, RetriesOnHole);
                        return Task.FromResult(false);
                    }
                    _logger.LogWarning("Skipping hole on {Position}", data.Position);
                }

                RetriesOnHole = 0;
                _stopOnHole = true;
                Position = data.Position;
                Processed++;
                return _subscription.OnNext(data);
            }

            public Task OnStart(long position)
            {
                _logger.LogDebug("OnStart({Position})", position);

                Position = position - 1;
                Processed = 0;
                _stopOnHole = RetriesOnHole < 5;
                return _subscription.OnStart(position);
            }

            public Task Completed(long position)
            {
                _logger.LogDebug("Completed({Position})", position);

                if (Processed > 0)
                {
                    Position = position;
                }

                return _subscription.Completed(position);
            }

            public Task Stopped(long position)
            {
                _logger.LogDebug("Stopped({Position})", position);
                return _subscription.Stopped(position);
            }

            public Task OnError(long position, Exception ex)
            {
                return _subscription.OnError(position, ex);
            }
        }

        private CancellationTokenSource _source;
        private readonly IPersistence _store;
        public int PollingIntervalMilliseconds { get; set; }
        public int HoleDetectionTimeout { get; set; }
        public long Position => _sequencer.Position;

        private readonly Sequencer _sequencer;
        private readonly ILogger _logger;
        private int _isPolling = 0;
        private bool _stopped = false;
        public PollingClient(IPersistence store, long lastPosition, ISubscription subscription, ILoggerFactory loggerFactory)
        {
            this._logger = loggerFactory.CreateLogger(GetType());

            _sequencer = new Sequencer(lastPosition, subscription, _logger);
            _store = store;
            PollingIntervalMilliseconds = 200;
            HoleDetectionTimeout = 2000;
        }

        public async Task Stop()
        {
            _source.Cancel();
            _source = null;

            while (_stopped == false)
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

            }, token).ConfigureAwait(false);
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
                    await InnerPolling(token);
                }
                finally
                {
                    Interlocked.Exchange(ref _isPolling, 0);
                }
            }
            //else
            //{
            //    throw new PollingException("Already polling");
            //}
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

                    if (_logger.IsEnabled(LogLevel.Debug))
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