using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Aggregates;
using NStore.InMemory;
using NStore.Raw;
using NStore.Sample.Domain.Room;
using NStore.Sample.Projections;
using NStore.Sample.Support;
using NStore.SnapshotStore;
using NStore.Streams;

namespace NStore.Sample
{
    public class SampleApp : IDisposable
    {
        private int _rooms;
        private readonly IRawStore _raw;

        private readonly IStreamStore _streams;
        private readonly IAggregateFactory _aggregateFactory;
        private readonly IReporter _reporter = new ColoredConsoleReporter("app", ConsoleColor.Gray);

        private CancellationTokenSource _source;
        private readonly AppProjections _appProjections;

        public SampleApp(IRawStore store)
        {
            _rooms = 32;
            _raw = store;
            _streams = new StreamStore(_raw);
            _aggregateFactory = new DefaultAggregateFactory();

            var network = new LocalAreaNetworkSimulator(10, 50);
            _appProjections = new AppProjections(network);

            Subscribe();
        }

        private IRepository GetRepository()
        {
            // return new IdentityMapRepositoryDecorator(new Repository(_aggregateFactory, _streams));
            // return new Repository(_aggregateFactory, _streams, new DefaultSnapshotStore(new InMemoryRawStore()));
            return new Repository(_aggregateFactory, _streams);
        }

        public void CreateRooms(int rooms)
        {
            _rooms = rooms;
            
            Enumerable.Range(1, _rooms).ForEachAsync(8, async i =>
            {
                var repository = GetRepository(); // repository is not thread safe!
                var id = GetRoomId(i);
                var room = await repository.GetById<Room>(id);

                room.EnableBookings();

                await repository.Save(room, id + "_create").ConfigureAwait(false);

                _reporter.Report($"Listed Room {id}");
            }).GetAwaiter().GetResult();
        }

        private string GetRoomId(int room)
        {
            return $"Room_{room:D3}";
        }

        private void Subscribe()
        {
            _source = new CancellationTokenSource();
            var token = _source.Token;

            Task.Run(async () =>
            {
                long lastScan = 0;
                while (!token.IsCancellationRequested)
                {
                    long nextScan = _appProjections.Position + 1;
                    if (lastScan != nextScan)
                    {
                        _reporter.Report($"Scanning store from #{nextScan}");
                        lastScan = nextScan;
                    }
                    
                    await this._raw.ScanStoreAsync(
                        nextScan,
                        ScanDirection.Forward,
                        _appProjections,
                        cancellationToken: token
                    );
                }
                await Task.Delay(50, token);
            }, token);
        }

        public void Dispose()
        {
            _source.Cancel();
        }

        public void ShowRooms()
        {
            _reporter.Report("Rooms:");
            foreach (var r in _appProjections.Rooms.List)
            {
                _reporter.Report($"  room => {r.Id}");
            }
        }

        public void AddSomeBookings(int bookings = 100)
        {
            var rnd = new Random(DateTime.Now.Millisecond);

            Enumerable.Range(1, bookings).ForEachAsync(8, async i =>
            {
                var id = GetRoomId(rnd.Next(_rooms) + 1);
                var fromDate = DateTime.Today.AddDays(rnd.Next(10));
                var toDate = fromDate.AddDays(rnd.Next(5));

                while (true)
                {
                    try
                    {
                        var repository = GetRepository(); // repository is not thread safe!
                        var room = await repository.GetById<Room>(id);

                        room.AddBooking(new DateRange(fromDate, toDate));

                        await repository.Save(room, Guid.NewGuid().ToString()).ConfigureAwait(false);
                        break;
                    }
                    catch (DuplicateStreamIndexException)
                    {
                        Console.WriteLine($"Concurrency exception on {id} => retry");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        break;
                    }
                }
            }).GetAwaiter().GetResult();
        }

        public void DumpMetrics()
        {
            this._appProjections.DumpMetrics();
        }
    }
}