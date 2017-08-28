using System;
using NStore.Core.Snapshots;

namespace NStore.Domain.Tests
{
    public class TicketSold
    {
    }

    public class TicketRefunded
    {
    }

    public class TicketState
    {
        public bool HasBeenSold { get; private set; }

        private void On(TicketSold e)
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
            if (snapshotInfo.SchemaVersion != "1")
                return null;

            return snapshotInfo;
        }
    }
}