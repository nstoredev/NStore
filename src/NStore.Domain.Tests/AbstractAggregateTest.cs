using System;
using System.Collections.Generic;
using NStore.Core.Snapshots;

namespace NStore.Domain.Tests
{
    public abstract class AbstractAggregateTest<TAggregate, TState> where TAggregate : IAggregate
    {
        private readonly IAggregateFactory _factory = new DefaultAggregateFactory();
        private TAggregate _aggregate;
        private IEventSourcedAggregate _accessor;

        protected TAggregate Aggregate
        {
            get
            {
                if (_aggregate == null)
                {
                    _aggregate = Create();
                    _accessor = (IEventSourcedAggregate)Aggregate;
                    _aggregate.Init("test_aggregate_id");
                }
                return _aggregate;
            }
        }

        protected IEnumerable<object> Events
        {
            get
            {
                var changeset = _accessor.GetChangeSet();
                return changeset.Events;
            }
        }

        protected TState State => (TState)(((ISnapshottable)Aggregate).GetSnapshot().Payload);

        protected void Setup(Action action)
        {
            action();
            var cs = _accessor.GetChangeSet();
            _accessor.Persisted(cs);
        }

        protected virtual TAggregate Create() => _factory.Create<TAggregate>();
    }
}