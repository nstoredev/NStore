using System;
using System.Collections.Generic;

namespace NStore.Domain
{
    public sealed class Changeset : IHeadersAccessor
    {
        public Object[] Events { get; private set; }
        public long AggregateVersion { get; private set; }
        public Dictionary<string, object> Headers { get; private set; }
        public bool IsEmpty => Events.Length == 0;

        private Changeset()
        {
            Headers = new Dictionary<string, object>();
        }

        public Changeset(long aggregateVersion, params object[] events) : this()
        {
            this.AggregateVersion = aggregateVersion;
            this.Events = events;
        }

        public IHeadersAccessor Add(string key, object value)
        {
            this.Headers.Add(key, value);
            return this;
        }
    }
}