using App.Metrics.Counter;

namespace NStore.LoadTests
{
    public static class Counters
    {
        public static readonly CounterOptions ReceivedMessages = new CounterOptions {Name = "Received Messages"};
        public static readonly CounterOptions SentMessages = new CounterOptions {Name = "Sent Messages"};
        public static readonly CounterOptions SimulatedMessages = new CounterOptions {Name = "Simulated Messages"};
    }
}