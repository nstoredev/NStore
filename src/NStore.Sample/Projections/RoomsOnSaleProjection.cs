using System.Collections.Generic;
using System.Threading.Tasks;
using NStore.Aggregates;
using NStore.Sample.Domain.Room;
using NStore.Sample.Support;

namespace NStore.Sample.Projections
{
    public class RoomsOnSaleProjection : AsyncProjector
    {
        private readonly IReporter _reporter;

        public class RoomsOnSale
        {
            public string Id { get; set; }
            public string RoomNumber { get; set; }
        }
        private readonly IDictionary<string, RoomsOnSale> _all = new Dictionary<string, RoomsOnSale>();

        public RoomsOnSaleProjection(IReporter reporter)
        {
            _reporter = reporter;
        }

        public IEnumerable<RoomsOnSale> List => _all.Values;

        public async Task On(RoomMadeAvailable e)
        {
            _all.Add(e.Id, new RoomsOnSale { Id = e.Id });
            await Task.Delay(100).ConfigureAwait(false);
        }
    }
}