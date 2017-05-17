using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CSharp.RuntimeBinder;
using NStore.Aggregates;

namespace NStore.Sample.Domain.Room
{
    public class RoomState : AggregateState, IInvariantsChecker
    {
        private readonly IList<DateRange> _reservations = new List<DateRange>();
        public bool BookingsEnabled { get; private set; }

        public void On(BookingsEnabled e)
        {
            this.BookingsEnabled = true;
        }

        public void On(RoomBooked e)
        {
            _reservations.Add(e.Dates);
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