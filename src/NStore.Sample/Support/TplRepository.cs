using System.Threading;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using NStore.Core.Streams;
using NStore.Domain;
using NStore.Tpl;

namespace NStore.Sample.Support
{
    public class TplRepository : Repository
    {
        public TplRepository(IAggregateFactory factory, IStreamsFactory streams) : base(factory, streams)
        {
        }

        public TplRepository(IAggregateFactory factory, IStreamsFactory streams, ISnapshotStore snapshots) : base(factory, streams, snapshots)
        {
        }

        protected override ISubscription ConfigureConsumer(ISubscription consumer, CancellationToken token)
        {
            return new TplSubscription(consumer, token);
        }
    }
}
