using System;

namespace NStore.Domain
{
	public interface IScheduledAt 
	{
		Object Payload { get; }
		DateTime At { get; }
	}

    public class ScheduledAt<T> : IScheduledAt
	{
        public ScheduledAt(T payload, DateTime at)
        {
            this.Payload = payload;
            this.At = at;
        }

        public T Payload { get; private set; }

		object IScheduledAt.Payload => Payload;

        public DateTime At { get; private set; }
    }
}