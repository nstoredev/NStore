using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class PollingClient
    {
        internal class Reader : ISubscription
        {
            private readonly ISubscription _subscription;
            public long Position { get; private set; } = 0;
            
            
            public Reader(ISubscription subscription)
            {
                _subscription = subscription;
            }

            public Task<bool> OnNext(IChunk data)
            {
                if (data.Position != Position + 1)
                    return Task.FromResult(false);

                Position = data.Position;
                return _subscription.OnNext(data);
            }

            public Task Completed()
            {
                return Task.CompletedTask;
            }

            public Task OnError(Exception ex)
            {
                return Task.CompletedTask;
            }
        }

        private CancellationTokenSource _source;
        private readonly IPersistence _store;
        public int PollingIntervalMilliseconds { get; set; }
        public int MissingChunksTimeoutMilliseconds { get; set; }

        public long Position => _reader.Position;

        private readonly Reader _reader;

        public PollingClient(IPersistence store, ISubscription subscription)
        {
            _reader = new Reader(subscription);
            _store = store;
            PollingIntervalMilliseconds = 200;
            MissingChunksTimeoutMilliseconds = 0;
        }

        public void Stop()
        {
            _source.Cancel();
        }

        public void Start()
        {
            _source = new CancellationTokenSource();
            var token = _source.Token;

            Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    await Poll(token);
                    await Task.Delay(PollingIntervalMilliseconds, token);
                }
            }, token);
        }

        public Task Poll()
        {
            return Poll(CancellationToken.None);
        }

        public async Task Poll(CancellationToken token)
        {
            await this._store.ReadAllAsync(
                Position + 1,
                _reader,
                int.MaxValue,
                token
            );
        }

        public Func<long, Task> OnMissingChunk { get; set; }
    }
}