using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NStore.Aggregates;
using NStore.Raw;
using NStore.Sample.Support;

namespace NStore.Sample.Projections
{
    public class AppProjections : IStoreObserver
    {
        private readonly IReporter _reporter;
        public long Position { get; set; } = 0;
        public RoomsOnSaleProjection Rooms { get; }
        public ConfirmedBookingsProjection Bookings { get; }

        private IList<IAsyncProjector> _projections = new List<IAsyncProjector>();
        public AppProjections(IReporter reporter)
        {
            _reporter = reporter;
            Rooms = new RoomsOnSaleProjection(reporter);
            Bookings = new ConfirmedBookingsProjection(reporter);
            Setup();
        }

        private void Report(string message)
        {
            _reporter.Report("prjengine", message);
        }

        public ScanCallbackResult Observe(
            long storeIndex,
            string streamId,
            long partitionIndex,
            object payload)
        {
            if (storeIndex != Position + 1)
            {
                Report($"Projection out of sequence {storeIndex} => wait next poll");
                return ScanCallbackResult.Stop;
            }

            Position = storeIndex;
            Report($"Projection: {storeIndex}");

            Changeset changes = (Changeset)payload;
            foreach (var e in changes.Events)
            {
                Task.WaitAll
                (
                    _projections.Select(p => p.Project(e)).ToArray()
                );
            }

            return ScanCallbackResult.Continue;
        }

        private void Setup()
        {
            _projections.Add(Rooms);
            _projections.Add(Bookings);
        }
    }
}