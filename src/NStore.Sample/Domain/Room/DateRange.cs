using System;

namespace NStore.Sample.Domain.Room
{
    public class InvalidDateRangeException : Exception
    {
        public InvalidDateRangeException(DateTime from, DateTime to) : base($"Invalid interval {from}-{to}")
        {
        }
    }

    public class DateRange
    {
        public DateTime From { get; private set; }
        public DateTime To { get; private set; }

        public DateRange(DateTime from, DateTime to)
        {
            if (from > to)
                throw new InvalidDateRangeException(from,to);

            this.From = from;
            this.To = to;
        }

        public bool Overlaps(DateRange range)
        {
            return this.Equals(range) ||
                   this.From < range.To && this.To > range.From;
        }

        protected bool Equals(DateRange other)
        {
            return From.Equals(other.From) && To.Equals(other.To);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((DateRange)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (From.GetHashCode() * 397) ^ To.GetHashCode();
            }
        }
    }
}