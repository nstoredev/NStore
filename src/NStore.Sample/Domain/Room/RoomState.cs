using System.Collections;
using System.Collections.Generic;
using System.Linq;
using NStore.Aggregates;

namespace NStore.Sample.Domain.Room
{
    public class RoomState : AggregateState
    {
        private readonly IList<DateRange> _reservations = new List<DateRange>();

        public void On(RoomBooked e)
        {
            _reservations.Add(e.Dates);
        }

        public bool IsAvailableOn(DateRange range)
        {
            return !_reservations.Any(range.Overlaps);
        }
    }
}