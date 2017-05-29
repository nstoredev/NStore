using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class PollingClient
    {
        private CancellationTokenSource _source;
        private readonly IPersistence _store;
        private readonly IAllPartitionsConsumer _consumer;
        public int Delay { get; set; }
        long _lastScan = 0;

        public long Position => _lastScan;

        public PollingClient(IPersistence store, IAllPartitionsConsumer consumer)
        {
            _consumer = consumer;
            _store = store;
            Delay = 200;
        }

        public void Stop()
        {
            _source.Cancel();
        }

        public void Start()
        {
            _source = new CancellationTokenSource();
            var token = _source.Token;

            var wrapper = new LambdaAllPartitionsConsumer((storeIndex, streamId, streamIndex, payload) =>
            {
                // retry if out of sequence
                if (storeIndex != _lastScan+1)
                    return Task.FromResult(ScanAction.Stop);

                _lastScan = storeIndex;
                return _consumer.Consume(storeIndex, streamId, streamIndex, payload);
            });

            Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    await this._store.ReadAllAsync(
                        _lastScan + 1,
                        ReadDirection.Forward,
                        wrapper,
                        int.MaxValue,
                        token
                    );
                    await Task.Delay(Delay, token);
                }
            }, token);
        }
    }
}
