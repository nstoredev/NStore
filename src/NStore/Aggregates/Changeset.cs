using System;
using System.Collections.Generic;

namespace NStore.Aggregates
{
    public sealed class Changeset : IHeadersAccessor
    {
        public Object[] Events { get; private set; }
        public long Version { get; private set; }
        public IDictionary<string, object> Headers { get; private set; }
        public bool IsEmpty => Events.Length == 0;

        private Changeset()
        {
            Headers = new Dictionary<string, object>();
        }

        public Changeset(long version, params object[] events) : this()
        {
            this.Version = version;
            this.Events = events;
        }

        public IHeadersAccessor Add(string key, object value)
        {
            this.Headers.Add(key, value);
            return this;
        }
    }
}