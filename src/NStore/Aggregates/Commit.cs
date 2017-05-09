using System;

namespace NStore.Aggregates
{
    public sealed class Commit
    {
        public Object[] Events { get; private set; }
        public long Version { get; private set; }
        public bool IsEmpty => Events.Length == 0;

        private Commit()
        {

        }

        public Commit(long version, params object[] events)
        {
            this.Version = version;
            this.Events = events;
        }
    }
}