using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class PollingClient
    {
        internal abstract class Command
        {
        }

        internal class PollCommand : Command
        {
        }

        internal class Reader : ISubscription
        {
            private readonly ISubscription _subscription;
            private readonly int _missingChunkMsTimeout;
            public long Position { get; private set; } = 0;
            private DateTime _lastProcessedTs = DateTime.UtcNow;

            public Func<long, Task> OnMissingChunk { get; set; }

            public Reader(ISubscription subscription, int missingChunkMsTimeout)
            {
                _subscription = subscription;
                _missingChunkMsTimeout = missingChunkMsTimeout;
            }

            public Task<bool> OnNext(IChunk data)
            {
//                if (data.Position != Position + 1)
//                {
//                    return Task.FromResult(false);
//                }
                
                if (data.Position != Position + 1)
                {
                    var elapsed = DateTime.UtcNow - _lastProcessedTs;
                    
                    if (elapsed.TotalMilliseconds >= _missingChunkMsTimeout)
                    {
                        Position = Position + 1;
                        _lastProcessedTs = DateTime.UtcNow;
                        
                        OnMissingChunk?.Invoke(Position);
                        
//                        return Task.FromResult(true);
                    }

                    return Task.FromResult(false);
                }

                _lastProcessedTs = DateTime.UtcNow;
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

        public long Position => _reader.Position;

        private readonly Reader _reader;

        public PollingClient(IPersistence store, ISubscription subscription)
        {
            _reader = new Reader(subscription, 500);
            _store = store;
            PollingIntervalMilliseconds = 200;
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
            
            // handle zero reads (holes)
        }

        public Func<long, Task> OnMissingChunk => _reader.OnMissingChunk;
    }
}