using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Sample.Domain.Room;
using NStore.Sample.Support;

namespace NStore.Sample.Projections
{
    public class ConfirmedBookingsProjection : AbstractProjection
    {
        private readonly IReporter _reporter;
        private readonly INetworkSimulator _networkSimulator;

        public ConfirmedBookingsProjection(IReporter reporter, INetworkSimulator networkSimulator)
        {
            this._reporter = reporter;
            _networkSimulator = networkSimulator;
        }

        public async Task On(BookingsEnabled e)
        {
            var elapsed = await _networkSimulator.WaitFast().ConfigureAwait(false);
            this._reporter.Report($"Room available {e.Id} took {elapsed}ms");
        }

        public async Task On(RoomBooked e)
        {
            var elapsed = await _networkSimulator.WaitFast().ConfigureAwait(false);
            this._reporter.Report($"Confirmed booking on {e.Id} took {elapsed}ms");
        }
    }
}