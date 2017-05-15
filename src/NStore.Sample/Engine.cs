using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Aggregates;
using NStore.InMemory;
using NStore.Raw;
using NStore.Sample.Domain.Room;
using NStore.Streams;

namespace NStore.Sample
{
    public class SampleApp : IDisposable
    {
        private readonly IRawStore _raw;

        private readonly IStreamStore _streams;
        private readonly IAggregateFactory _aggregateFactory;
        private CancellationTokenSource _source;
        private Projections _projections = new Projections();

		public SampleApp()
        {
            _raw = new InMemoryRawStore(new LatencySimulator(200));
            _streams = new StreamStore(_raw);
            _aggregateFactory = new DefaultAggregateFactory();
            Subscribe();
        }

        private IRepository GetRepository()
        {
            return new Repository(_aggregateFactory, _streams);
        }

        public void CreateRooms()
        {
            var batch = Enumerable.Range(1, 10).Select( async i =>
            {
                var repository = GetRepository(); // repository is not thread safe!
                var id = "Room_" + i;
                var room = await repository.GetById<Room>(id);
                room.MakeAvailable();
                await repository.Save(room, id + "_create").ConfigureAwait(false);
                Console.WriteLine($"Listed Room {id}");
            }).ToArray();

            Task.WaitAll(batch);
        }

        private void Subscribe()
        {
            _source = new CancellationTokenSource();
            var token = _source.Token;

            Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    await this._raw.ScanStoreAsync(
                        _projections.Position + 1,
                        ScanDirection.Forward,
                        _projections,
                        cancellationToken: token
                    );
                }
                await Task.Delay(50);
            });
        }

        public void Dispose()
        {
            _source.Cancel();
        }

		public void ShowRooms()
		{
            Console.WriteLine("Rooms:");
            foreach(var r in _projections.Rooms.List)
            {
                Console.WriteLine($"  room => {r.Id}");
            }
        }
	}
}