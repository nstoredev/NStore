using System;
using NStore.Aggregates;
using NStore.Sample.Domain.Room;
using NStore.SnapshotStore;
using Xunit;

namespace NStore.Sample.Tests
{
    public abstract class AbstractAggregateTest<TAggregate, TState>
        where TAggregate : IAggregate where TState : AggregateState, new()
    {
        private readonly IAggregateFactory _defaultFactory = new DefaultAggregateFactory();
        protected TState State { get; private set; }
        protected TAggregate Aggregate { get; private set; }

        protected AbstractAggregateTest()
        {
            this.Aggregate = _defaultFactory.Create<TAggregate>();
            this.State = new TState();
            var snapshot = new SnapshotInfo("test", 1, this.State, this.State.GetStateVersion());
            ((IEventSourcedAggregate)this.Aggregate).TryRestore(snapshot);

            if(!this.Aggregate.IsInitialized)
                throw new Exception("something went wrong");
        }

        protected void Setup(Action action)
        {
            action();
            // clear changes
            var persiter = Aggregate as IEventSourcedAggregate;

            if (persiter != null)
            {
                var cs = persiter.GetChangeSet();
                persiter.ChangesPersisted(cs);
            }
        }
    }

    public class RoomTests : AbstractAggregateTest<Room, RoomState>
    {
        protected Room Room => Aggregate;

        [Fact]
        public void new_room_is_not_enabled_for_bookings()
        {
            Assert.False(State.BookingsEnabled);
            Assert.True(Room.CheckInvariants());
        }

        [Fact]
        public void can_be_made_available()
        {
            Room.EnableBookings();

            Assert.True(State.BookingsEnabled);
            Assert.True(Room.CheckInvariants());
        }

        [Fact]
        public void bookings_can_be_enabled_multiple_times()
        {
            Setup(() => { Room.EnableBookings(); });

            Room.EnableBookings();

            Assert.True(State.BookingsEnabled);
            Assert.True(Room.CheckInvariants());
        }
    }
}