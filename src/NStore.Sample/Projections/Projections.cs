using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NStore.Aggregates;
using NStore.InMemory;
using NStore.Raw;
using NStore.Sample.Support;

namespace NStore.Sample.Projections
{
    public class AppProjections : IStoreObserver
    {
        public long Position { get; set; } = 0;
        public RoomsOnSaleProjection Rooms { get; }
        public ConfirmedBookingsProjection Bookings { get; }

        private readonly IList<IProjection> _projections = new List<IProjection>();
        private readonly IReporter _reporter = new ColoredConsoleReporter("projections", ConsoleColor.Yellow);

        public AppProjections(INetworkSimulator network)
        {
            Rooms = new RoomsOnSaleProjection(new ColoredConsoleReporter("rooms on sale", ConsoleColor.Red), network);
            Bookings = new ConfirmedBookingsProjection(new ColoredConsoleReporter("confirmed bookings", ConsoleColor.Cyan), network);
            Setup();
        }

        public ScanCallbackResult Observe(
            long storeIndex,
            string streamId,
            long partitionIndex,
            object payload)
        {
            if (storeIndex != Position + 1)
            {
                _reporter.Report($"!!!!!!!!!!!!!!!! Projection out of sequence {storeIndex} => wait next poll !!!!!!!!!!!!!!!!");
                return ScanCallbackResult.Stop;
            }

            Position = storeIndex;

            Changeset changes = (Changeset)payload;

            var sw = new Stopwatch();
            sw.Start();
            Task.WaitAll
            (
                _projections.Select(p => p.Project(changes)).ToArray()
            );

            _reporter.Report($"dispatched changeset #{storeIndex} took {sw.ElapsedMilliseconds}ms");
            return ScanCallbackResult.Continue;
        }

        private void Setup()
        {
            _projections.Add(Rooms);
            _projections.Add(Bookings);
        }
    }
}