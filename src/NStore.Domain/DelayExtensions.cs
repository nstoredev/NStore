using System;

namespace NStore.Domain
{
    public static class DelayExtensions
    {
        public static ScheduledAt<T> HappensAfter<T>(this T payload, DateTime at)
        {
            return new ScheduledAt<T>(payload, at);
        }
    }
}