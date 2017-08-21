using System.Collections;
using System.Collections.Generic;
using System.Linq;
using NStore.Aggregates;

namespace NStore.Sample.Domain.Room
{
    public class RoomState : AggregateState, IInvariantsChecker
    {
        private readonly IList<DateRange> _reservations = new List<DateRange>();
        public bool BookingsEnabled { get; private set; }
        public int MissedBookings { get; private set; }

        private void On(BookingsEnabled e)
        {
            this.BookingsEnabled = true;
        }

        private void On(RoomBooked e)
        {
            _reservations.Add(e.Dates);
        }

        private void On(RoomBookingFailed e)
        {
            MissedBookings++;
        }

        public bool IsAvailableOn(DateRange range)
        {
            return !_reservations.Any(range.Overlaps);
        }

        public bool CheckInvariants()
        {
            return !(!BookingsEnabled && _reservations.Any());
        }
    }
}