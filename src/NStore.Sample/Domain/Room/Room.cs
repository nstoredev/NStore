using System;
using NStore.Aggregates;

namespace NStore.Sample.Domain.Room
{
    public class RoomMadeAvailable
    {
        public string Id { get; private set; }

        public RoomMadeAvailable(string id)
        {
            this.Id = id;
        }
    }

    public class DateRange
    {
        public DateTime From { get; private set; }
        public DateTime To { get; private set; }

        public DateRange(DateTime from, DateTime to)
        {
            this.From = from;
            this.To = to;
        }
    }

    public class Room : Aggregate<RoomState>
    {
        public void MakeAvailable()
        {
            Raise(new RoomMadeAvailable(this.Id));
        }

        public void AddBooking(DateRange dates)
        {
            Raise(new RoomBooked(this.Id, dates));
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