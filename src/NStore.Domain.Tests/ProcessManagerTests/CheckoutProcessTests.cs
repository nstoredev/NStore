using System;
using System.Collections.Generic;
using NStore.Core.Snapshots;
using Xunit;

namespace NStore.Domain.Tests.ProcessManagerTests
{
    public class AbstractProcessManagerTest<TProcess, TState>
        where TProcess : IAggregate where TState : class, new()
    {
        private readonly IAggregateFactory _defaultFactory = new DefaultAggregateFactory();
        protected TProcess Process { get; private set; }
        protected TState State { get; private set; }

        public AbstractProcessManagerTest()
        {
            Process = _defaultFactory.Create<TProcess>();
            State = new TState();

            //restore @ version 1
            var snapshot = new SnapshotInfo("test", 1, State, "1");
            ((ISnapshottable)this.Process).TryRestore(snapshot);

            if (!this.Process.IsInitialized)
#pragma warning disable S112 // General exceptions should never be thrown
                throw new Exception("something went wrong");
#pragma warning restore S112 // General exceptions should never be thrown            
        }

        protected void Setup(Action action)
        {
            action();
            // clear changes
            var persiter = Process as IEventSourcedAggregate;

            if (persiter != null)
            {
                var cs = persiter.GetChangeSet();
                persiter.Persisted(cs);
                persiter.Loaded();
            }
        }

        protected Changeset GetChangeset()
        {
            var persiter = Process as IEventSourcedAggregate;
            return persiter.GetChangeSet();
        }

        protected object[] GetMessagesOut()
        {
            var changeset = GetChangeset();
            var list = new List<object>();
            foreach (var changesetEvent in changeset.Events)
            {
                var wrapped = (MessageReaction) changesetEvent;
                list.AddRange(wrapped.MessagesOut);
            }

            return list.ToArray();
        }
    }

    public class CheckoutProcessTests : AbstractProcessManagerTest<CheckoutProcess, CheckoutState>
    {
        private CheckoutProcess Checkout => Process;

        [Fact]
        public void placing_an_order_should_trigger_payment_request_and_notification()
        {
            Checkout.MessageReceived(new OrderPlaced("Order_1"));

            Assert.Collection(
                GetMessagesOut(),
                o => Assert.IsType<MessageAndTimeout<RequestPayment>>(o),
                o => Assert.IsType<SendPurchaseConfirmation>(o)
            );
        }

        [Fact]
        public void payment_received_should_change_state()
        {
            Checkout.MessageReceived(new PaymentReceived("Order_1", DateTime.Parse("2017-12-31")));
            Assert.True(State.PaymentReceived);
            Assert.Collection(
                GetMessagesOut(),
                o => Assert.IsType<ShipOrder>(o)
            );
        }

        [Fact]
        public void shipping_should_change_state()
        {
            Setup(() =>
            {
                Process.MessageReceived(new OrderPlaced("Order_1"));
                Process.MessageReceived(new PaymentReceived("Order_1", DateTime.Parse("2017-12-31")));
            });

            Checkout.MessageReceived(new OrderShipped("Order_1"));

            Assert.Empty(GetMessagesOut());
            Assert.True(State.Shipped);
        }

        [Fact]
        public void changeset_should_include_outputs()
        {
            Checkout.MessageReceived(new PaymentReceived("Order_1", DateTime.Parse("2017-12-31")));

            var changeset = GetChangeset();

            var wrapped = changeset.Events[0] as MessageReaction;

            Assert.NotNull(wrapped);
            Assert.Collection(
                wrapped.MessagesOut,
                o => Assert.IsType<ShipOrder>(o)
            );
        }

        [Fact]
        public void process_can_apply_changeset()
        {
            Checkout.MessageReceived(new PaymentReceived("Order_1", DateTime.Parse("2017-12-31")));
            var changeset = GetChangeset();

            var cloned = new CheckoutProcess();
            var snapshot = new SnapshotInfo("test", 1, new CheckoutState(), "1");
            ((ISnapshottable)cloned).TryRestore(snapshot);

            ((IEventSourcedAggregate)cloned).ApplyChanges(changeset);

            Assert.True(cloned.ExposeStateForTest.PaymentReceived);
        }
    }
}
