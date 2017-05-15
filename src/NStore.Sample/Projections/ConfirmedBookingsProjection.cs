using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NStore.Aggregates;
using NStore.InMemory;
using NStore.Sample.Domain.Room;
using NStore.Sample.Support;

namespace NStore.Sample.Projections
{
    public class ConfirmedBookingsProjection : AsyncProjector
    {
        private readonly IReporter _reporter;
        private readonly IDelayer _delayer;

        public ConfirmedBookingsProjection(IReporter reporter, IDelayer delayer)
        {
            this._reporter = reporter;
            _delayer = delayer;
        }

        public async Task On(RoomMadeAvailable e)
        {
            var elapsed = await _delayer.Wait().ConfigureAwait(false);
            this._reporter.Report($"Room available {e.Id} took {elapsed}ms");
        }

        public async Task On(RoomBooked e)
        {
            var elapsed = await _delayer.Wait().ConfigureAwait(false);
            this._reporter.Report($"Confirmed booking on {e.Id} took {elapsed}ms");
        }
    }
}