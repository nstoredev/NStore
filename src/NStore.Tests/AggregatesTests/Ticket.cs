using System;
using NStore.Aggregates;
using NStore.SnapshotStore;

namespace NStore.Tests.AggregatesTests
{
    public class TicketSold
    {
    }

    public class TicketRefunded
    {
    }

    public class TicketState : AggregateState
    {
        public bool HasBeenSold { get; private set; }

        public void On(TicketSold e)
        {
            this.HasBeenSold = true;
        }
    }

    public class Ticket : Aggregate<TicketState>
    {
        public TicketState ExposedStateForTest => State;

        public void Sale()
        {
            if (State.HasBeenSold)
            {
                throw new Exception($"Ticket already sold");
            }

            Emit(new TicketSold());
        }

        public void Refund()
        {
            if(!State.HasBeenSold)
            {
                throw new Exception($"Cannot refund an unsold ticket");
            }

            Emit(new TicketRefunded());
        }

        public Changeset ExposePendingChanges()
        {
            return ((IEventSourcedAggregate) this).GetChangeSet();
        }

        protected override SnapshotInfo PreprocessSnapshot(SnapshotInfo snapshotInfo)
        {
            if (snapshotInfo.SnapshotVersion != 1)
                return null;

            return snapshotInfo;
        }
    }
}