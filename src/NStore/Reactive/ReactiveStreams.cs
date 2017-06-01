using System;
namespace NStore.Reactive
{
    // from reactive-streams .net
    //@@REVIEW add link & comments 
    public interface IPublisher<out T>
    {
		void Subscribe(ISubscriber<T> subscriber);
	}

    public interface ISubscriber<in T>
    {
		void OnSubscribe(ISubscription subscription);
		void OnNext(T element);
		void OnError(Exception cause);
		void OnComplete();
	}

	public interface ISubscription
	{
		void Request(long n);
		void Cancel();
	}
}
