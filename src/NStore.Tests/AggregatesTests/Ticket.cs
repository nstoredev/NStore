using System;
using System.Collections.Generic;
using NStore.Aggregates;
using NStore.SnapshotStore;
using Xunit;

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

            Raise(new TicketSold());
        }

        public void Refund()
        {
            if(!State.HasBeenSold)
            {
                throw new Exception($"Cannot refund an unsold ticket");
            }

            Raise(new TicketRefunded());
        }

        public Changeset ExposePendingChanges()
        {
            return ((IAggregatePersister) this).GetChangeSet();
        }

        protected override SnapshotInfo PreprocessSnapshot(SnapshotInfo snapshotInfo)
        {
            if (snapshotInfo.SnapshotVersion != 1)
                return null;

            return snapshotInfo;
        }
    }
}