namespace NStore.Domain.Tests
{
    public class CounterAggregateState
    {
        public int Value { get; private set; }

        private void On(CounterIncremented e)
        {
            Value++;
        }

        private void On(CounterDecremented e)
        {
            Value--;
        }
    }

    public class CounterIncremented
    {
    }

    public class CounterDecremented
    {
    }

    public class CounterAggregate : Aggregate<CounterAggregateState>, IInvariantsChecker
    {
        public void Increment()
        {
            Emit(new CounterIncremented());
        }

        public void Decrement()
        {
            Emit(new CounterDecremented());
        }

        public InvariantsCheckResult CheckInvariants()
        {
            return State.Value >= 0 ? 
                InvariantsCheckResult.Ok : 
                InvariantsCheckResult.Invalid("Counter is negative");
        }
    }
}