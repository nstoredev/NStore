using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
    public class PollingClient
    {
        private CancellationTokenSource _source;
        private readonly IRawStore _store;
        private readonly IStoreObserver _observer;
        public long Delay { get; set; }
	    long _lastScan = 0;

	    public long Position => _lastScan;
	    
        public PollingClient(IRawStore store, IStoreObserver observer)
        {
            _observer = observer;
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

			var wrapper = new LambdaStoreObserver((storeIndex,streamId, streamIndex, payload) => {
                _lastScan = storeIndex;
                return _observer.Observe(storeIndex,streamId,streamIndex, payload);
            });

			Task.Run(async () =>
			{
				while (!token.IsCancellationRequested)
				{
                    await this._store.ScanStoreAsync(
						_lastScan,
						ScanDirection.Forward,
						wrapper,
						cancellationToken: token
					);
					await Task.Delay(200, token);
				}
			}, token);
        }
    }
}
