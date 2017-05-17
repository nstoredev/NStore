using System.Resources;
using NStore.Aggregates;

namespace NStore.Sample.Domain.Room
{
    public class BookingsEnabled
    {
        public string Id { get; private set; }

        public BookingsEnabled(string id)
        {
            this.Id = id;
        }
    }

    public class Room : Aggregate<RoomState>, IInvariantsChecker
    {
        public void EnableBookings()
        {
            if(!this.State.BookingsEnabled)
                Raise(new BookingsEnabled(this.Id));
        }

        public void AddBooking(DateRange dates)
        {
            Raise(new RoomBooked(this.Id, dates));
        }

        public bool CheckInvariants()
        {
            return State.CheckInvariants();
        }
    }

    public class RoomBooked
    {
        public string Id { get; private set; }
        public DateRange Dates { get; private set; }

        public RoomBooked(string id, DateRange dates)
        {
            Id = id;
            Dates = dates;
        }
    }
}