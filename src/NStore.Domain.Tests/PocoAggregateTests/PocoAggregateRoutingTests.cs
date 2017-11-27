using System;
using System.Threading.Tasks;
using Xunit;

namespace NStore.Domain.Tests.PocoAggregateTests
{
    public class TurnOn
    {
    }

    public class TurnOff
    {
    }

    public class SwitchedOn
    {
    }

    public class SwitchedOff
    {
    }

    public delegate object Executor(object command);

    public class StateRouter
    {
        private Executor _state;

        public StateRouter(Executor state)
        {
            _state = state;
        }

        public void TransitionTo(Executor state)
        {
            _state = state;
        }

        public object Execute(object command)
        {
            return _state(command);
        }
    }

    public class LightBulb
    {
        private readonly StateRouter _router;

        public LightBulb()
        {
            _router = new StateRouter(StateOff);
        }

        private object StateOff(object command)
        {
            switch (command)
            {
                case TurnOn t: return new SwitchedOn();
            }
            throw new NotSupportedException();
        }

        private object StateOn(object command)
        {
            switch (command)
            {
                case TurnOff t: return new SwitchedOff();
            }
            throw new NotSupportedException();
        }

        public bool IsOn { get; private set; }

        private void On(SwitchedOn evt)
        {
            this.IsOn = true;
            _router.TransitionTo(StateOn);
        }

        private void On(SwitchedOff evt)
        {
            this.IsOn = false;
            _router.TransitionTo(StateOff);
        }

        public object OnCommand(object command)
        {
            return _router.Execute(command);
        }
    }

    public class PocoAggregateRoutingTests : AbstractPocoAggregateTest<LightBulb>
    {
        private IPocoAggregate LightBulb => Aggregate;

        [Fact]
        public void should_switch_on()
        {
            LightBulb.Do(new TurnOn());
            Assert.True(State.IsOn);
        }

        [Fact]
        public void should_switch_off()
        {
            Setup(() => { LightBulb.Do(new TurnOn()); });

            LightBulb.Do(new TurnOff());
            Assert.False(State.IsOn);
        }

        [Fact]
        public void should_throw_if_turned_on_twice()
        {
            Setup(() => { LightBulb.Do(new TurnOn()); });

            Assert.Throws<NotSupportedException>(() =>
                LightBulb.Do(new TurnOn())
            );
        }
    }
}