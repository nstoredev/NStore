using System;
using System.Threading.Tasks;

namespace NStore
{

	public enum ScanDirection
	{
		Forward,
		Backward
	}

	public enum ScanCallbackResult
	{
		Stop,
		Continue
	}

	public interface IStore
	{
		Task InitAsync();
		Task ScanAsync(string streamId, long indexStart, ScanDirection direction, Func<long, object, ScanCallbackResult> callback);
		Task PersistAsync(string streamId, long index, object payload, string operationId = null);
	}
}