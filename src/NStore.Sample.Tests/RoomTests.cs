using System;
using System.Collections.Generic;
using System.Linq;
using NStore.Core.Snapshots;
using NStore.Domain;
using NStore.Sample.Domain.Room;
using Xunit;

namespace NStore.Sample.Tests
{
    public abstract class AbstractAggregateTest<TAggregate, TState>
        where TAggregate : IAggregate where TState : class, new()
    {
        private readonly IAggregateFactory _defaultFactory = new DefaultAggregateFactory();
        protected TState State { get; private set; }
        protected TAggregate Aggregate { get; private set; }

        protected AbstractAggregateTest()
        {
            this.Aggregate = _defaultFactory.Create<TAggregate>();
            this.State = new TState();
            var snapshot = new SnapshotInfo("test", 1, this.State, "1");
            ((ISnapshottable)this.Aggregate).TryRestore(snapshot);

            if (!this.Aggregate.IsInitialized)
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
                persiter.Persisted(cs);
            }
        }

        protected IEnumerable<object> Events
        {
            get
            {
                var persiter = Aggregate as IEventSourcedAggregate;

                if (persiter != null)
                {
                    var cs = persiter.GetChangeSet();
                    return cs.Events;
                }

                return Enumerable.Empty<object>();
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
            Assert.False(Room.CheckInvariants().IsInvalid);
        }

        [Fact]
        public void can_be_made_available()
        {
            Room.EnableBookings();

            Assert.True(State.BookingsEnabled);
            Assert.False(Room.CheckInvariants().IsInvalid);
        }

        [Fact]
        public void bookings_can_be_enabled_multiple_times()
        {
            Setup(() => { Room.EnableBookings(); });

            Room.EnableBookings();

            Assert.True(State.BookingsEnabled);
            Assert.False(Room.CheckInvariants().IsInvalid);
        }

        [Fact]
        public void room_registration_should_emit_event()
        {
            Room.EnableBookings();

            Assert.Collection(Events, 
                e => Assert.IsType<BookingsEnabled>(e)
            );
        }

        [Fact]
        public void rooms_with_active_reservations_should_not_be_disabled()
        {
            Setup(() =>
            {
                Room.EnableBookings();
                Room.AddBooking(new DateRange(DateTime.Today, DateTime.Today.AddDays(1)));
            });

            Room.DisableBookings();

            var result = Room.CheckInvariants();

            Assert.True(result.IsInvalid);
            Assert.Equal("Room has beed disabled with active reservations", result.Message);
        }
    }
}