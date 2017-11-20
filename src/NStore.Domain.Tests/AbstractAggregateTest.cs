using System;
using System.Collections.Generic;
using NStore.Core.Snapshots;

namespace NStore.Domain.Tests
{
    public abstract class AbstractAggregateTest<TAggregate, TState> where TAggregate : IAggregate
    {
        protected readonly TAggregate Aggregate;
        protected readonly IAggregateFactory Factory = new DefaultAggregateFactory();
        private readonly IEventSourcedAggregate _accessor;

        protected AbstractAggregateTest()
        {
            Aggregate = Factory.Create<TAggregate>();
            _accessor = (IEventSourcedAggregate) Aggregate;
            Aggregate.Init("mage");
        }

        protected IEnumerable<object> Events
        {
            get
            {
                var changeset = _accessor.GetChangeSet();
                return changeset.Events;
            }
        }

        protected TState State => (TState) (((ISnapshottable) Aggregate).GetSnapshot().Payload);

        protected void Setup(Action action)
        {
            action();
            var cs = _accessor.GetChangeSet();
            _accessor.Persisted(cs);
        }
    }
}