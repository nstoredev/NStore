using System;
using System.Collections.Generic;

namespace NStore.Domain
{
    /// <summary>
    /// Changes of the aggregate, saved atomically (all-or-nothing)
    /// 
    /// !!!!!!! DANGER ZONE !!!!!!!
    ///
    /// Class is sealed AND MUST NOT BE SERIALIZED in your store.
    /// Why this is good:
    ///   - implementation can change anytime without breaking your data
    ///   - breaking changes will be detected at compile time and not in production
    /// 
    /// </summary>
    public sealed class Changeset : IHeadersAccessor
    {
        public Object[] Events { get; private set; }
        public long AggregateVersion { get; private set; }
        public Dictionary<string, object> Headers { get; private set; }

        private Changeset()
        {
            Headers = new Dictionary<string, object>();
        }

        public Changeset(long aggregateVersion, object[] events) : this()
        {
            this.AggregateVersion = aggregateVersion;
            this.Events = events;
        }

        public Changeset(long aggregateVersion, object[] events, Dictionary<string, object> headers)
        {
            this.AggregateVersion = aggregateVersion;
            this.Events = events;
            this.Headers = headers;
        }

        public IHeadersAccessor Add(string key, object value)
        {
            this.Headers.Add(key, value);
            return this;
        }

        public bool IsEmpty() => Events.Length == 0;
    }
}