using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NStore.Aggregates;
using NStore.Sample.Domain.Room;
using NStore.Sample.Support;

namespace NStore.Sample.Projections
{
    public class ConfirmedBookingsProjection : AsyncProjector
    {
        private readonly IReporter _reporter;

        public ConfirmedBookingsProjection(IReporter reporter)
        {
            this._reporter = reporter;
        }

        public async Task On(RoomBooked e)
        {
            this._reporter.Report("ConfirmedBookingsProjection", $"Confirmed booking on {e.Id}");
            await Task.Delay(100).ConfigureAwait(false);
        }
    }
}