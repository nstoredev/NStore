using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class PollingClient
    {
        private class Reader : ISubscription
        {
            private readonly ISubscription _subscription;
            public long Position { get; private set; } = 0;
            private DateTime _lastProcessedTs = DateTime.UtcNow;

            public Func<long, Task> OnMissingChunk { get; set; }
            public bool DumpMessages { get; set; }
            public long Processed { get; private set; }
            public bool HoleDetected { get; private set; }
            private bool _stopOnHole = true;

            public Reader(ISubscription subscription)
            {
                _subscription = subscription;
            }

            public Task<bool> OnNext(IChunk data)
            {
                if (DumpMessages)
                    Console.WriteLine($"OnNext({data.Position})");

                if (data.Position != Position + 1)
                {
                    if(_stopOnHole){
						HoleDetected = true;
						return Task.FromResult(false);
					}
                }

                _lastProcessedTs = DateTime.UtcNow;
                Position = data.Position;
                Processed++;
                return _subscription.OnNext(data);
            }

            public Task OnStart(long position)
            {
                if (DumpMessages)
                    Console.WriteLine($"OnStart({position})");

                Position = position - 1;
                Processed = 0;

                _stopOnHole = !this.HoleDetected;

                this.HoleDetected = false;
                return Task.CompletedTask;
            }

            public Task Completed(long position)
            {
                if (DumpMessages)
                    Console.WriteLine($"Completed({position})");
                Position = position;
                return Task.CompletedTask;
            }

            public Task Stopped(long position)
            {
                if (DumpMessages)
                    Console.WriteLine($"Stopped({position})");
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

        public bool DumpMessages
        {
            get => _reader.DumpMessages;
            set => _reader.DumpMessages = value;
        }

        public PollingClient(IPersistence store, ISubscription subscription)
        {
            _reader = new Reader(subscription);
            _store = store;
            PollingIntervalMilliseconds = 200;
            HoleDetectionTimeout = 400;
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
            do
            {
				if (DumpMessages)
					Console.WriteLine("--------------------------------------");
				
                var start = Position + 1;
				await this._store.ReadAllAsync(
					start,
					_reader,
					int.MaxValue,
					token
				);

				if (_reader.HoleDetected)
				{
					await Task.Delay(HoleDetectionTimeout);
				}

                if (DumpMessages)
				{
                    Console.WriteLine($"HoleDetected : {_reader.HoleDetected}");
					Console.WriteLine($"Polled from {start} to {_reader.Position}, processed {_reader.Processed} chunks");
					Console.WriteLine("--------------------------------------");
				}
			} 
            while (_reader.HoleDetected);
        }

        public Func<long, Task> OnMissingChunk => _reader.OnMissingChunk;
    }
}