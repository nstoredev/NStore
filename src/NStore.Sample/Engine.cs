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
    public class Engine : IDisposable
    {
        private readonly IRawStore _raw;

        private readonly IStreamStore _streams;
        private readonly IAggregateFactory _aggregateFactory;
        private CancellationTokenSource _source;
        private Projections _projections = new Projections();

		public Engine()
        {
            _raw = new InMemoryRawStore();
            _streams = new StreamStore(_raw);
            _aggregateFactory = new DefaultAggregateFactory();
            Subscribe();
        }

        private IRepository GetRepository()
        {
            return new Repository(_aggregateFactory, _streams);
        }

        public async Task CreateRooms()
        {
            var repository = GetRepository();
            foreach (var i in Enumerable.Range(1, 5))
            {
                var id = "Room_" + i;
                var room = await repository.GetById<Room>(id);
                room.MakeAvailable();
                await repository.Save(room, id + "_create");
                Console.WriteLine($"Listed Room {id}");
            }
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

                    await Task.Delay(100);
                }
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