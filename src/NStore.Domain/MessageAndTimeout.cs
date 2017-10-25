using System;

namespace NStore.Domain
{
	public interface IMessageAndTimeout
	{
		Object Message { get; }

		TimeSpan Delay { get; }

		int Counter { get; }

		String Target { get; }

		bool SendMessageOut { get; }

		bool SentToSelf { get; }

		bool IsValid { get; }
	}

	public class MessageAndTimeout<T> : IMessageAndTimeout
	{
		public T Message { get; private set; }

		object IMessageAndTimeout.Message => Message;

		public TimeSpan Delay { get; private set; }
		public int Counter { get; private set; }
		public string Target { get; private set; }
		public bool SendMessageOut { get; private set; }

		public bool SentToSelf => this.Target == "@self";
		public bool IsValid => !String.IsNullOrWhiteSpace(this.Target) && (this.Delay != TimeSpan.Zero);

		public MessageAndTimeout(T message) : this(message, TimeSpan.Zero)
		{
		}

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

		public MessageAndTimeout<T> After(TimeSpan delay)
		{
			this.Delay = delay;
			return this;
		}

		public MessageAndTimeout<T> ToSelf()
		{
			this.Target = "@self";
			return this;
		}

		public MessageAndTimeout<T> RetryTimeoutAfter(TimeSpan ts)
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
		public static MessageAndTimeout<T> AndSignalTimeout<T>(this T payload)
		{
			return new MessageAndTimeout<T>(payload);
		}
	}
}