using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using NStore.Aggregates;
using NStore.Persistence;
using NStore.SnapshotStore;
using NStore.Streams;
using NStore.Tpl;

namespace NStore.Sample.Support
{
    public class TplRepository : Repository
    {
        public TplRepository(IAggregateFactory factory, IStreamStore streams) : base(factory, streams)
        {
        }

        public TplRepository(IAggregateFactory factory, IStreamStore streams, ISnapshotStore snapshots) : base(factory, streams, snapshots)
        {
        }

        protected override ISubscription ConfigureConsumer(ISubscription consumer, CancellationToken token)
        {
            return new TplSubscription(consumer, token);
        }
    }
}
