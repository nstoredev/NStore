using System;
using System.Collections.Generic;
using NStore.Aggregates;
using NStore.Raw;
using NStore.Sample.Domain.Room;

namespace NStore.Sample
{
    public class Projections : IStoreObserver
    {
        public long Position { get; set; } = 0;
        public RoomsOnSaleProjection Rooms { get; } = new RoomsOnSaleProjection();
        private IList<IProjector> _projections = new List<IProjector>();
        public Projections()
        {
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
                Console.WriteLine($"Projection out of sequence {storeIndex} => wait next poll");
                return ScanCallbackResult.Stop;
            }

            Position = storeIndex;
            Console.WriteLine($"Projection: {storeIndex}");

            Changeset changes = (Changeset)payload;

            foreach (var p in _projections)
            {
                foreach (var e in changes.Events)
                {
                    p.Project(e);
                }
            }

            return ScanCallbackResult.Continue;
        }

        private void Setup()
        {
            _projections.Add(Rooms);
        }
    }

    public class RoomsOnSaleProjection : AbstractProjector
    {
        public class RoomsOnSale
        {
            public string Id { get; set; }
            public string RoonNumber { get; set; }
        }
        private IDictionary<string, RoomsOnSale> _all = new Dictionary<string, RoomsOnSale>();

        public IEnumerable<RoomsOnSale> List => _all.Values;

        public void On(RoomMadeAvailable e)
        {
            _all.Add(e.Id, new RoomsOnSale { Id = e.Id });
        }
    }
}