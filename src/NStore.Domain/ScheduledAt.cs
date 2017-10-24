using System;

namespace NStore.Domain
{
    public class ScheduledAt<T>
    {
        public ScheduledAt(T payload, DateTime at)
        {
            this.Payload = payload;
            this.At = at;
        }

        public T Payload { get; private set; }
        public DateTime At { get; private set; }
    }
}