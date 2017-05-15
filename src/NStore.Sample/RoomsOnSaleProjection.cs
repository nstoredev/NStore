using System.Collections.Generic;
using NStore.Aggregates;
using NStore.Sample.Domain.Room;

namespace NStore.Sample
{
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