using System;

namespace NStore.Domain
{
    public class MessageAndTimeout<T>
    {
        public T Message { get; private set; }
        public TimeSpan Delay { get; private set; }
        public int Counter { get; private set; }
        public string Target { get; private set; }
        public bool SendMessageOut { get; private set; }

        public bool SentToSelf => this.Target == "@self";
        public bool IsValid => !String.IsNullOrWhiteSpace(this.Target);

        public MessageAndTimeout(T message, TimeSpan delay)
        {
            Message = message;
            SendMessageOut = true;

            Delay = delay;
            Counter = 1;
        }

        public MessageAndTimeout<T> To(string target)
        {
            this.Target = target;
            return this;
        }

        public MessageAndTimeout<T> ToSelf()
        {
            this.Target = "@self";
            return this;
        }

        public MessageAndTimeout<T> RetryTimeout(TimeSpan ts)
        {
            return new MessageAndTimeout<T>(this.Message, ts)
            {
                Counter = this.Counter + 1,
                Target = this.Target,
                SendMessageOut = false
            };
        }

        public MessageAndTimeout<T> RetryMessageAndTimeout(TimeSpan ts)
        {
            return new MessageAndTimeout<T>(this.Message, ts)
            {
                Counter = this.Counter + 1,
                Target = this.Target,
                SendMessageOut = true
            };
        }
    }

    public static class TimeoutExtensions
    {
        public static MessageAndTimeout<T> AndSignalTimeoutAfter<T>(this T payload, TimeSpan ts)
        {
            return new MessageAndTimeout<T>(payload, ts);
        }
    }
}