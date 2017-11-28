using System;
using System.Threading.Tasks;
using Xunit;

namespace NStore.Domain.Tests.PocoAggregateTests
{
    public class TurnOn { }
    public class TurnOff { }
    public class SwitchedOn { }
    public class SwitchedOff { }

    public class LightBulb
    {
        private readonly StateRouter _state;

        public LightBulb()
        {
            _state = new StateRouter()
                .Define("On", StateOn)
                .Define("Off", StateOff)
                .Start("Off");
        }

        private object StateOff(object command)
        {
            switch (command)
            {
                case TurnOn t: return new SwitchedOn();
            }
            return _state.Unhandled(command);
        }

        private object StateOn(object command)
        {
            switch (command)
            {
                case TurnOff t: return new SwitchedOff();
            }
            return _state.Unhandled(command);
        }

        public bool IsOn { get; private set; }

        private void On(SwitchedOn evt)
        {
            this.IsOn = true;
            _state.TransitionTo("On");
        }

        private void On(SwitchedOff evt)
        {
            this.IsOn = false;
            _state.TransitionTo("Off");
        }

        public object Execute(object command)
        {
            return _state.Execute(command);
        }
    }

    public class PocoAggregateRoutingTests : AbstractPocoAggregateTest<LightBulb>
    {
        private IPocoAggregate LightBulb => Aggregate;

        protected override PocoAggregate<LightBulb> Create()
        {
            return new PocoAggregate<LightBulb>(new ExecuteProcessor());
        }

        [Fact]
        public void should_switch_on()
        {
            LightBulb.Do(new TurnOn());
            Assert.True(State.IsOn);
        }

        [Fact]
        public void should_eventually_turned_on()
        {
            LightBulb.Do(new TurnOn());
            LightBulb.Do(new TurnOff());
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