using NStore.Domain;

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

    public class BookingsDisabled
    {
        public string Id { get; private set; }

        public BookingsDisabled(string id)
        {
            this.Id = id;
        }
    }

    public class Room : Aggregate<RoomState>, IInvariantsChecker
    {
        public void EnableBookings()
        {
            if(!this.State.BookingsEnabled)
                Emit(new BookingsEnabled(this.Id));
        }

        public void DisableBookings()
        {
            if (this.State.BookingsEnabled)
                Emit(new BookingsDisabled(this.Id));
        }

        public void AddBooking(DateRange dates)
        {
            if (State.IsAvailableOn(dates))
            {
                Emit(new RoomBooked(this.Id, dates));
            }
            else
            {
                Emit(new RoomBookingFailed(this.Id, dates));
            }
        }

        public InvariantsCheckResult CheckInvariants()
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

    public class RoomBookingFailed
    {
        public string Id { get; private set; }
        public DateRange Dates { get; private set; }

        public RoomBookingFailed(string id, DateRange dates)
        {
            Id = id;
            Dates = dates;
        }
    }
}