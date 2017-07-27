using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NStore.Persistence
{
    public class PollingClient
    {
        private class Reader : ISubscription
        {
            private readonly ISubscription _subscription;
            private readonly ILogger _logger;
            public long Position { get; private set; } = 0;

            public long Processed { get; private set; }
            public int RetriesOnHole { get; private set; }
            private bool _stopOnHole = true;

            public Reader(ISubscription subscription, ILogger logger)
            {
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
                return Task.CompletedTask;
            }

            public Task Completed(long position)
            {
                _logger.LogDebug("Completed({Position})", position);

                Position = position;
                return Task.CompletedTask;
            }

            public Task Stopped(long position)
            {
                _logger.LogDebug("Stopped({Position})", position);
                return Task.CompletedTask;
            }

            public Task OnError(long position, Exception ex)
            {
                return Task.CompletedTask;
            }
        }

        private CancellationTokenSource _source;
        private readonly IPersistence _store;
        public int PollingIntervalMilliseconds { get; set; }
        public int HoleDetectionTimeout { get; set; }
        public long Position => _reader.Position;

        private readonly Reader _reader;
        private readonly ILogger _logger;

        public PollingClient(IPersistence store, ISubscription subscription, ILoggerFactory loggerFactory)
        {
            this._logger = loggerFactory.CreateLogger(GetType());

            _reader = new Reader(subscription, _logger);
            _store = store;
            PollingIntervalMilliseconds = 200;
            HoleDetectionTimeout = 2000;
        }

        public void Stop()
        {
            _source.Cancel();
            _source = null;
        }

        public void Start()
        {
            _source = new CancellationTokenSource();
            var token = _source.Token;

            Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    await Poll(token).ConfigureAwait(false);
                    await Task.Delay(PollingIntervalMilliseconds, token).ConfigureAwait(false);
                }
            }, token).ConfigureAwait(false);
        }

        public Task Poll()
        {
            return Poll(CancellationToken.None);
        }

        public async Task Poll(CancellationToken token)
        {
            do
            {
                using (_logger.BeginScope("Poll"))
                {
                    var start = Position + 1;
                    await this._store.ReadAllAsync(
                        start,
                        _reader,
                        int.MaxValue,
                        token
                    ).ConfigureAwait(false);

                    if (_reader.RetriesOnHole > 0)
                    {
                        await Task.Delay(HoleDetectionTimeout, token).ConfigureAwait(false);
                    }

                    _logger.LogDebug($"HoleDetected : {_reader.RetriesOnHole}");
                    _logger.LogDebug($"Polled from {start} to {_reader.Position}, processed {_reader.Processed} chunks");
                }
            }
            while (_reader.RetriesOnHole > 0);
        }
    }
}